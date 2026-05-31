use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use arrayvec::ArrayString;
use async_trait::async_trait;
use bytes::Bytes;
use scraper::{Html, Selector};
use thiserror::Error;
use url::Url;

use databus::{
    message::{Message, MessageBuilder},
    producer::Producer,
};

pub struct HtmlListProducer<const STR_CAP: usize> {
    target_url: Url,
    tree_path_selector: Selector,
    sub_path_selector: Selector,
    ingest_from_back: bool,

    _marker: PhantomData<[u8; STR_CAP]>,
}

#[derive(Clone)]
pub struct HtmlListProducerState {
    pub last_extracted_url: Option<String>,
}

#[derive(Error, Debug)]
pub enum HtmlListProducerError {
    #[error("Failed to create HtmlListProducer: {0}")]
    CreationError(Cow<'static, str>),

    #[error("Failed to load URL: {0}")]
    UrlLoadError(Cow<'static, str>),

    #[error("Failed to extract data: {0}")]
    ExtractError(Cow<'static, str>),
}

impl<const STR_CAP: usize> HtmlListProducer<STR_CAP> {
    pub fn new(
        target_url: &str,
        tree_path: &str,
        sub_path: &str,
        ingest_from_back: bool,
    ) -> Result<Self, HtmlListProducerError> {
        let target_url = Url::parse(target_url).map_err(|e| {
            HtmlListProducerError::CreationError(
                format!("Invalid target URL '{}': {}", target_url, e).into(),
            )
        })?;
        let tree_path_selector = Selector::parse(tree_path).map_err(|e| {
            HtmlListProducerError::CreationError(
                format!("Invalid tree path selector '{}': {}", tree_path, e).into(),
            )
        })?;
        let sub_path_selector = Selector::parse(sub_path).map_err(|e| {
            HtmlListProducerError::CreationError(
                format!("Invalid URL path selector '{}': {}", sub_path, e).into(),
            )
        })?;

        Ok(Self {
            target_url,
            tree_path_selector,
            sub_path_selector,
            ingest_from_back,

            _marker: PhantomData,
        })
    }

    fn extract(&self, html_content: &str) -> Vec<String> {
        let document = Html::parse_document(html_content);

        document
            .select(&self.tree_path_selector)
            .flat_map(|element| element.select(&self.sub_path_selector))
            .filter_map(|sub_element| sub_element.value().attr("href"))
            .map(|href| href.to_string())
            .collect()
    }

    async fn extract_from_url(&self, url: &Url) -> Result<Vec<String>, HtmlListProducerError> {
        let response = reqwest::get(url.as_str()).await.map_err(|e| {
            HtmlListProducerError::UrlLoadError(format!("Failed to load URL: {}", e).into())
        })?;

        let content = response.text().await.map_err(|e| {
            HtmlListProducerError::UrlLoadError(format!("Failed to read response: {}", e).into())
        })?;

        Ok(self.extract(&content))
    }

    async fn read_from_url(&self, url: &Url) -> Result<Bytes, HtmlListProducerError> {
        let response = reqwest::get(url.as_str()).await.map_err(|e| {
            HtmlListProducerError::UrlLoadError(format!("Failed to load URL: {}", e).into())
        })?;

        let content = response.text().await.map_err(|e| {
            HtmlListProducerError::UrlLoadError(format!("Failed to read response: {}", e).into())
        })?;

        Ok(Bytes::from(content))
    }

    fn get_filename_from_url(&self, url: &Url) -> Result<String, HtmlListProducerError> {
        url.path_segments()
            .and_then(|segments| segments.last())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                HtmlListProducerError::ExtractError(
                    "Failed to extract dataset name from URL".into(),
                )
            })
    }

    async fn get_dataset_and_filename_from_url(
        &self,
        url: &Url,
    ) -> Result<(Bytes, String), HtmlListProducerError> {
        let content = self.read_from_url(url).await?;
        let filename = self.get_filename_from_url(url)?;

        Ok((content, filename))
    }
}

#[async_trait]
impl<const STR_CAP: usize> Producer<Bytes, HtmlListProducerState, STR_CAP>
    for HtmlListProducer<STR_CAP>
{
    async fn produce(
        &self,
        topic: ArrayString<STR_CAP>,
        old_state: HtmlListProducerState,
    ) -> (Arc<Message<Bytes>>, HtmlListProducerState) {
        let links = self
            .extract_from_url(&self.target_url)
            .await
            .map_err(|_| HtmlListProducerError::ExtractError("Could not extract from url".into()));

        let mut links = match links {
            Ok(l) => l,
            Err(_) => {
                return (
                    Arc::new(
                        MessageBuilder::<Bytes>::new_empty(Bytes::new())
                            .unwrap()
                            .build(),
                    ),
                    HtmlListProducerState {
                        last_extracted_url: old_state.last_extracted_url,
                    },
                );
            }
        };

        if links.is_empty() {
            return (
                Arc::new(
                    MessageBuilder::<Bytes>::new_empty(Bytes::new())
                        .unwrap()
                        .build(),
                ),
                HtmlListProducerState {
                    last_extracted_url: old_state.last_extracted_url,
                },
            );
        }

        if self.ingest_from_back {
            links.reverse();
        }

        let latest_link = {
            let checkpoint = old_state.last_extracted_url.as_ref();

            let target_index = match checkpoint {
                Some(c) => match links.iter().position(|link| link == c) {
                    Some(i) => i + 1,
                    None => 0,
                },
                None => 0,
            };

            match links.get(target_index) {
                Some(l) => l.clone(),
                None => {
                    return (
                        Arc::new(
                            MessageBuilder::<Bytes>::new_empty(Bytes::new())
                                .unwrap()
                                .build(),
                        ),
                        HtmlListProducerState {
                            last_extracted_url: old_state.last_extracted_url,
                        },
                    );
                }
            }
        };

        let latest_url = match Url::parse(&latest_link) {
            Ok(url) => url,
            Err(_) => {
                return (
                    Arc::new(
                        MessageBuilder::<Bytes>::new_empty(Bytes::new())
                            .unwrap()
                            .build(),
                    ),
                    HtmlListProducerState {
                        last_extracted_url: old_state.last_extracted_url,
                    },
                );
            }
        };

        let (content, dataset_name) =
            match self.get_dataset_and_filename_from_url(&latest_url).await {
                Ok(result) => result,
                Err(_) => {
                    return (
                        Arc::new(
                            MessageBuilder::<Bytes>::new_empty(Bytes::new())
                                .unwrap()
                                .build(),
                        ),
                        HtmlListProducerState {
                            last_extracted_url: old_state.last_extracted_url,
                        },
                    );
                }
            };

        (
            Arc::new(
                MessageBuilder::<Bytes>::new_data(content)
                    .unwrap()
                    .add_meta("filename", &dataset_name)
                    .build(),
            ),
            HtmlListProducerState {
                last_extracted_url: Some(latest_link),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_extract_returns_matching_links_only() {
        let producer = HtmlListProducer::<20>::new(
            "https://example.com",
            "ul.list-class",
            "a.link-class",
            false,
        )
        .unwrap();

        let mock_html = r#"
            <html>
                <body>
                    <ul class="list-class">
                        <li><a class="link-class" href="/page1">One</a></li>
                        <li><a class="link-class" href="/page2">Two</a></li>
                    </ul>
                    <div class="other-class">
                        <a class="link-class" href="/ignored">Ignored</a>
                    </div>
                </body>
            </html>
        "#;

        let results = producer.extract(mock_html);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "/page1");
        assert_eq!(results[1], "/page2");
    }

    #[test]
    fn test_extract_skips_elements_without_href() {
        let producer = HtmlListProducer::<20>::new(
            "https://example.com",
            "ul.list-class",
            "a.link-class",
            false,
        )
        .unwrap();

        let mock_html = r#"
            <html>
                <body>
                    <ul class="list-class">
                        <li><a class="link-class">Missing href</a></li>
                        <li><a class="link-class" href="/page2">Two</a></li>
                    </ul>
                </body>
            </html>
        "#;

        let results = producer.extract(mock_html);

        assert_eq!(results, vec!["/page2".to_string()]);
    }

    #[test]
    fn test_new_rejects_invalid_target_url() {
        let error = match HtmlListProducer::<20>::new(
            "not-a-url",
            "ul.list-class",
            "a.link-class",
            false,
        ) {
            Ok(_) => panic!("expected invalid URL"),
            Err(error) => error,
        };

        assert!(matches!(error, HtmlListProducerError::CreationError(_)));
        assert!(error.to_string().contains("Invalid target URL 'not-a-url'"));
    }

    #[test]
    fn test_new_rejects_invalid_tree_path_selector() {
        let error = match HtmlListProducer::<20>::new(
            "https://example.com",
            "ul.list-class[",
            "a.link-class",
            false,
        ) {
            Ok(_) => panic!("expected invalid tree selector"),
            Err(error) => error,
        };

        assert!(matches!(error, HtmlListProducerError::CreationError(_)));
        assert!(
            error
                .to_string()
                .contains("Invalid tree path selector 'ul.list-class['")
        );
    }

    #[test]
    fn test_new_rejects_invalid_sub_path_selector() {
        let error = match HtmlListProducer::<20>::new(
            "https://example.com",
            "ul.list-class",
            "a.link-class[",
            false,
        ) {
            Ok(_) => panic!("expected invalid sub-path selector"),
            Err(error) => error,
        };

        assert!(matches!(error, HtmlListProducerError::CreationError(_)));
        assert!(
            error
                .to_string()
                .contains("Invalid URL path selector 'a.link-class['")
        );
    }

    #[test]
    fn test_get_filename_from_url_returns_last_path_segment() {
        let producer = HtmlListProducer::<20>::new(
            "https://example.com",
            "ul.list-class",
            "a.link-class",
            false,
        )
        .unwrap();
        let url = Url::parse("https://example.com/datasets/report.csv").unwrap();

        let filename = producer.get_filename_from_url(&url).unwrap();

        assert_eq!(filename, "report.csv");
    }

    #[test]
    fn test_get_filename_from_url_rejects_urls_without_path_segments() {
        let producer = HtmlListProducer::<20>::new(
            "https://example.com",
            "ul.list-class",
            "a.link-class",
            false,
        )
        .unwrap();
        let url = Url::parse("mailto:test@example.com").unwrap();

        let error = producer
            .get_filename_from_url(&url)
            .expect_err("expected filename extraction to fail");

        assert!(matches!(error, HtmlListProducerError::ExtractError(_)));
        assert_eq!(
            error.to_string(),
            "Failed to extract data: Failed to extract dataset name from URL"
        );
    }
}
