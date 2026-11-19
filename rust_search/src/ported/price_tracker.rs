//! Smart Price Tracker for Egypt - compares prices from Amazon Egypt, Jumia, Noon, Souq
//! Uses regex-based extraction from crawled pages

use regex::Regex;
use scraper::{Html, Selector};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceInfo {
    pub store: String,
    pub price: f64,
    pub currency: String,
    pub url: String,
    pub product_name: Option<String>,
}

struct StoreConfig {
    name: String,
    url_pattern: Regex,
    price_selectors: Vec<String>,
    currency: String,
    product_selector: Option<String>,
}

pub struct PriceTracker {
    egyptian_stores: Vec<StoreConfig>,
}

impl PriceTracker {
    pub fn new() -> Self {
        Self {
            egyptian_stores: vec![
                StoreConfig {
                    name: "Amazon Egypt".to_string(),
                    url_pattern: Regex::new(r"amazon\.eg").unwrap(),
                    price_selectors: vec![
                        ".a-price-whole".to_string(),
                        ".a-price .a-offscreen".to_string(),
                        "[data-price]".to_string(),
                        ".a-color-price".to_string(),
                    ],
                    currency: "EGP".to_string(),
                    product_selector: Some("#productTitle".to_string()),
                },
                StoreConfig {
                    name: "Jumia Egypt".to_string(),
                    url_pattern: Regex::new(r"jumia\.eg").unwrap(),
                    price_selectors: vec![
                        ".current-price".to_string(),
                        ".price".to_string(),
                        "[data-price]".to_string(),
                        ".my8".to_string(),
                    ],
                    currency: "EGP".to_string(),
                    product_selector: Some(".product-title".to_string()),
                },
                StoreConfig {
                    name: "Noon Egypt".to_string(),
                    url_pattern: Regex::new(r"noon\.com").unwrap(),
                    price_selectors: vec![
                        ".priceNow".to_string(),
                        ".price_".to_string(),
                        "[data-price]".to_string(),
                        ".c26".to_string(),
                    ],
                    currency: "EGP".to_string(),
                    product_selector: Some(".productTitle".to_string()),
                },
                StoreConfig {
                    name: "Souq Egypt".to_string(),
                    url_pattern: Regex::new(r"souq\.com").unwrap(),
                    price_selectors: vec![
                        ".price".to_string(),
                        ".nowPrice".to_string(),
                        "[data-price]".to_string(),
                    ],
                    currency: "EGP".to_string(),
                    product_selector: Some(".product-title".to_string()),
                },
                StoreConfig {
                    name: "AliExpress Egypt".to_string(),
                    url_pattern: Regex::new(r"aliexpress\.com").unwrap(),
                    price_selectors: vec![
                        ".product-price-value".to_string(),
                        ".price".to_string(),
                        "[data-price]".to_string(),
                    ],
                    currency: "EGP".to_string(),
                    product_selector: Some(".product-title".to_string()),
                },
            ],
        }
    }

    pub fn extract_price(&self, html: &str, url: &str) -> Option<PriceInfo> {
        for store in &self.egyptian_stores {
            if store.url_pattern.is_match(url) {
                let document = Html::parse_document(html);
                
                let product_name = if let Some(selector_str) = &store.product_selector {
                    if let Ok(selector) = Selector::parse(selector_str) {
                        document.select(&selector).next().map(|el| el.text().collect::<String>().trim().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                for selector_str in &store.price_selectors {
                    if let Ok(selector) = Selector::parse(selector_str) {
                        if let Some(element) = document.select(&selector).next() {
                            let price_text = element.text().collect::<String>();
                            if let Some(price) = parse_egyptian_price(&price_text) {
                                return Some(PriceInfo {
                                    store: store.name.clone(),
                                    price,
                                    currency: store.currency.clone(),
                                    url: url.to_string(),
                                    product_name,
                                });
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub fn compare_prices(&self, query: &str, pages: &[serde_json::Value]) -> Vec<PriceInfo> {
        let mut results = Vec::new();
        
        for page in pages {
            if let Some(url) = page.get("url").and_then(|v| v.as_str()) {
                if let Some(content) = page.get("content").and_then(|v| v.as_str()) {
                    if let Some(price_info) = self.extract_price(content, url) {
                        // Check if the product name matches the query
                        if let Some(ref product) = price_info.product_name {
                            let product_lower = product.to_lowercase();
                            let query_lower = query.to_lowercase();
                            if product_lower.contains(&query_lower) || query_lower.contains(&product_lower) {
                                results.push(price_info);
                            }
                        } else {
                            results.push(price_info);
                        }
                    }
                }
            }
        }
        
        results.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
        results
    }
}

fn parse_egyptian_price(text: &str) -> Option<f64> {
    let replaced = text
        .replace("EGP", "")
        .replace("ج.م", "")
        .replace("£", "")
        .replace("LE", "")
        .replace("E£", "")
        .replace(" ", "")
        .replace(",", "")
        .replace("=", "");
    replaced.trim().parse::<f64>().ok()
}

impl Default for PriceTracker {
    fn default() -> Self {
        Self::new()
    }
}