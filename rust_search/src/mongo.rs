use std::{env, time::Duration};

use futures_util::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, Document},
    options::{ClientOptions, IndexOptions},
    Client as MongoClient, Collection, IndexModel,
};
use regex::escape;

use crate::state::{SEARCH_FIELDS, SearchResult};

pub(crate) async fn create_mongo_collection()
-> Option<(Collection<Document>, Collection<Document>, bool)> {
    let uri = match env::var("MONGO_URI") {
        Ok(uri) if !uri.trim().is_empty() => uri,
        _ => return None,
    };

    let mut options = match ClientOptions::parse(&uri).await {
        Ok(options) => options,
        Err(error) => {
            eprintln!("Rust search could not configure MongoDB: {error}");
            return None;
        }
    };
    options.server_selection_timeout = Some(Duration::from_secs(8));
    options.connect_timeout = Some(Duration::from_secs(8));
    let client = match MongoClient::with_options(options) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("Rust search could not configure MongoDB: {error}");
            return None;
        }
    };
    if let Err(error) = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
    {
        eprintln!("Rust search could not connect to MongoDB: {error}");
        return None;
    }

    let database = match env::var("MONGO_DATABASE").or_else(|_| env::var("MONGO_DB")) {
        Ok(name) if !name.trim().is_empty() => client.database(&name),
        _ => match client.default_database() {
            Some(database) => database,
            None => {
                eprintln!("Set MONGO_DATABASE or include a database name in MONGO_URI");
                return None;
            }
        },
    };
    let collection_name = env::var("MONGO_SEARCH_COLLECTION")
        .or_else(|_| env::var("MONGO_COLLECTION"))
        .unwrap_or_else(|_| "Crawled_Pages".to_owned());
    let collection = database.collection::<Document>(&collection_name);
    let queue_name =
        env::var("MONGO_QUEUE_COLLECTION").unwrap_or_else(|_| "velo_crawl_queue".to_owned());
    let queue = database.collection::<Document>(&queue_name);
    let queue_url_index = IndexModel::builder()
        .keys(doc! { "url": 1 })
        .options(
            IndexOptions::builder()
                .name("velo_queue_url_idx".to_owned())
                .unique(true)
                .build(),
        )
        .build();
    if let Err(error) = queue.create_index(queue_url_index).await {
        eprintln!("Could not ensure MongoDB crawl queue URL uniqueness: {error}");
    }
    let text_index = has_mongo_text_index(&collection).await;
    if !text_index {
        eprintln!("MongoDB text index is missing; Rust search will use the keyword fallback");
    }
    Some((collection, queue, text_index))
}

pub(crate) async fn has_mongo_text_index(collection: &Collection<Document>) -> bool {
    let mut indexes = match collection.list_indexes().await {
        Ok(indexes) => indexes,
        Err(error) => {
            eprintln!("Could not inspect MongoDB search indexes: {error}");
            return false;
        }
    };
    loop {
        match indexes.try_next().await {
            Ok(Some(index)) => {
                let weights = index
                    .options
                    .as_ref()
                    .and_then(|options: &IndexOptions| options.weights.as_ref());
                if weights.is_some_and(|weights| {
                    SEARCH_FIELDS
                        .iter()
                        .all(|field| weights.contains_key(*field))
                }) {
                    return true;
                }
            }
            Ok(None) => return false,
            Err(error) => {
                eprintln!("Could not read MongoDB search indexes: {error}");
                return false;
            }
        }
    }
}

pub(crate) fn mongo_match(terms: &[String], use_text_index: bool) -> Document {
    let mut clauses: Vec<Bson> = terms
        .iter()
        .map(|term| {
            let fields: Vec<Bson> = SEARCH_FIELDS
                .iter()
                .map(|field| {
                    let mut condition = Document::new();
                    condition.insert(
                        *field,
                        doc! {
                            "$regex": escape(term),
                            "$options": "i"
                        },
                    );
                    Bson::Document(condition)
                })
                .collect();
            Bson::Document(doc! { "$or": fields })
        })
        .collect();
    if use_text_index && terms.iter().all(|term| term.chars().count() >= 3) {
        clauses.push(Bson::Document(doc! {
            "$text": {
                "$search": terms.join(" "),
                "$language": "none"
            }
        }));
    }
    doc! { "$and": clauses }
}

fn mongo_score_terms(terms: &[String]) -> Vec<Bson> {
    terms
        .iter()
        .flat_map(|term| {
            SEARCH_FIELDS.iter().enumerate().map(move |(index, field)| {
                let weight = match index {
                    0 => 5,
                    1 => 2,
                    _ => 1,
                };
                Bson::Document(doc! {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": { "$ifNull": [format!("${field}"), ""] },
                                "regex": escape(term),
                                "options": "i"
                            }
                        },
                        weight,
                        0
                    ]
                })
            })
        })
        .collect()
}

fn bson_string(row: &Document, field: &str) -> String {
    row.get_str(field).unwrap_or_default().to_owned()
}

fn bson_score(row: &Document) -> f64 {
    match row.get("score") {
        Some(Bson::Double(value)) => *value,
        Some(Bson::Int32(value)) => f64::from(*value),
        Some(Bson::Int64(value)) => *value as f64,
        _ => 0.0,
    }
}

pub(crate) async fn search_mongo(
    collection: Option<Collection<Document>>,
    has_text_index: bool,
    terms: &[String],
    limit: usize,
    offset: usize,
) -> (Vec<SearchResult>, u64) {
    let Some(collection) = collection else {
        return (Vec::new(), 0);
    };
    if terms.is_empty() {
        return (Vec::new(), 0);
    }

    let filter = mongo_match(terms, has_text_index);
    let total = match collection.count_documents(filter.clone()).await {
        Ok(total) => total,
        Err(error) => {
            eprintln!("MongoDB search count failed: {error}");
            return (Vec::new(), 0);
        }
    };
    let score = mongo_score_terms(terms);
    let pipeline = vec![
        doc! { "$match": filter },
        doc! { "$addFields": { "score": { "$add": score } } },
        doc! { "$sort": { "score": -1, "crawled_at": -1 } },
        doc! { "$skip": offset as i64 },
        doc! { "$limit": limit as i64 },
        doc! {
            "$project": {
                "url": 1,
                "title": 1,
                "description": 1,
                "content": 1,
                "score": 1
            }
        },
    ];
    let rows = match collection.aggregate(pipeline).await {
        Ok(cursor) => match cursor.try_collect::<Vec<Document>>().await {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("MongoDB search aggregation failed: {error}");
                return (Vec::new(), 0);
            }
        },
        Err(error) => {
            eprintln!("MongoDB search aggregation failed: {error}");
            return (Vec::new(), 0);
        }
    };
    let results = rows
        .iter()
        .map(|row| SearchResult {
            url: bson_string(row, "url"),
            title: bson_string(row, "title"),
            description: bson_string(row, "description"),
            content: bson_string(row, "content"),
            score: bson_score(row),
            favicon: None,
            source: "MongoDB Atlas",
        })
        .collect();
    (results, total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mongo_filter_requires_each_term_and_fields() {
        let filter = mongo_match(&["alpha".into(), "beta".into()], false);
        let clauses = filter.get_array("$and").unwrap();
        assert_eq!(clauses.len(), 2);
        assert!(clauses.iter().all(|clause| {
            clause
                .as_document()
                .and_then(|document| document.get_array("$or").ok())
                .is_some_and(|fields| fields.len() == SEARCH_FIELDS.len())
        }));
    }
}
