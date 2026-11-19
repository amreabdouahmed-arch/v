use anyhow::Result;
use mongodb::options::ClientOptions;
use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};

use crate::ported::cloud::mongo_collection;

/// Async MongoDB utility equivalent to `mongodb_helper.py`.
pub struct MongoDbHelper {
    client: Option<Client>,
    collection: Option<Collection<Document>>,
}

impl MongoDbHelper {
    pub async fn connect() -> Result<Self> {
        let (client, _database, collection) = mongo_collection(
            &["MONGO_DATABASE", "MONGO_DB", "MONGODB_DB"],
            &[
                "MONGO_SEARCH_COLLECTION",
                "MONGO_COLLECTION",
                "MONGODB_COLLECTION",
            ],
            "Crawled_Pages",
        )
        .await?;
        Ok(Self {
            client: Some(client),
            collection: Some(collection),
        })
    }

    pub async fn save_page(
        &self,
        url: &str,
        title: &str,
        content: &str,
        crawler_name: Option<&str>,
    ) -> Result<bool> {
        let Some(collection) = &self.collection else {
            return Ok(false);
        };
        let name = crawler_name
            .map(str::to_owned)
            .or_else(|| std::env::var("CRAWLER_NAME").ok())
            .unwrap_or_else(|| "unknown".to_owned());
        let result = collection
            .update_one(
                doc! { "url": url },
                doc! { "$set": {
                    "url": url,
                    "title": title,
                    "content": content,
                    "crawler_name": name,
                    "crawled_at": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64(),
                }},
            )
            .upsert(true)
            .await;
        Ok(result.is_ok())
    }

    pub async fn count_pages(&self, crawler_name: Option<&str>) -> u64 {
        let Some(collection) = &self.collection else {
            return 0;
        };
        let filter = crawler_name
            .map(|name| doc! { "crawler_name": name })
            .unwrap_or_default();
        collection.count_documents(filter).await.unwrap_or(0)
    }

    pub fn close(mut self) {
        drop(self.collection.take());
        drop(self.client.take());
    }
}

/// Construct a client directly when a caller needs a non-default collection.
pub async fn connect_uri(uri: &str) -> Result<Client> {
    Ok(Client::with_options(ClientOptions::parse(uri).await?)?)
}
