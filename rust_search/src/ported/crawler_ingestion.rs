//! Rust ports of the legacy crawler and ingestion entry points.
//!
//! Built-in copies of all four legacy hard-coded target catalogs are exposed
//! by [`seed_catalog`]. Implement [`CloudPageWriter`],
//! [`DurableCrawlQueue`], and [`TaskQueue`] in the host application to wire
//! the legacy cloud stores and durable queue to the existing Rust service.
//! This module does not create a local database.
//!
//! To expose this module, add `pub mod crawler_ingestion;` under the existing
//! `ported` module declaration. Adapt the current cloud store to
//! [`CloudPageWriter`] (mapping each [`CrawlerKind`] to the old table/schema
//! and honoring its [`ConflictPolicy`]); adapt the store's queue methods to
//! [`DurableCrawlQueue`]; then call [`crawl`] or [`run_search_queue`]. A Redis
//! or other broker adapter implements [`TaskQueue`] and [`TaskBackend`].
//! Advanced targets retain their per-domain cap and `extract_price` flag on
//! each seed. The Python runner's subprocess loop has a concrete Tokio-based
//! replacement in [`run_all_crawlers`] and [`supervise_all_crawlers`].
//!
//! Build dependencies required by this file (not added to Cargo manifests
//! here): `reqwest` with `json`, `stream`, `rustls-tls`, `gzip`, `deflate`,
//! and `brotli`; `scraper`; `encoding_rs`; and Tokio's `time` and `sync`
//! features. The
//! repository already uses `futures-util`, `regex`, `serde`, `serde_json`,
//! and `url`.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{stream, FutureExt, StreamExt};
use regex::Regex;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

pub const USER_AGENT: &str = "VeloSearchBot/1.0 (+https://example.org/velo-bot)";
const LEGACY_BROWSER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

const ADVANCED_CATALOG: &str = r#"
1|بحث_عربي|arabic_search|300|false|https://ar.wikipedia.org|https://mawdoo3.com|https://www.marefa.org|https://www.almaany.com|https://www.mosoah.com|https://www.youm7.com|https://www.masrawy.com|https://www.almasryalyoum.com|https://www.shorouknews.com|https://gate.ahram.org.eg|https://www.dostor.org|https://www.elwatannews.com|https://www.akhbarelyom.com|https://www.aljazeera.net|https://www.alarabiya.net|https://www.skynewsarabia.com|https://www.sabq.org|https://www.okaz.com.sa|https://www.alriyadh.com|https://www.khaleejtimes.com|https://www.gulfnews.com|https://arabic.cnn.com|https://www.bbc.com/arabic|https://arabic.rt.com|https://www.france24.com/ar|https://www.dw.com/ar|https://www.alhurra.com|https://www.hespress.com|https://www.echoroukonline.com|https://www.el-khabar.com|https://www.babnet.net|https://islamqa.info/ar|https://www.islamweb.net/ar|https://quran.com/ar|https://dorar.net|https://www.alukah.net|https://www.hindawi.org|https://www.abjjad.com|https://www.adab.com|https://www.alfaseeh.com|https://www.arabsgate.com|https://www.fatakat.com|https://www.hawaaworld.com
2|بحث_عالمي|global_search|200|false|https://www.wikipedia.org|https://www.britannica.com|https://www.nationalgeographic.com|https://www.smithsonianmag.com|https://www.sciencedaily.com|https://www.livescience.com|https://www.space.com|https://www.nature.com|https://arxiv.org|https://www.bbc.com|https://www.reuters.com|https://www.nytimes.com|https://www.theguardian.com|https://www.bloomberg.com|https://www.cnn.com|https://www.theatlantic.com|https://www.vox.com|https://www.wired.com|https://techcrunch.com|https://www.theverge.com
3|مراقبة_أسعار|price_monitoring|500|true|https://www.amazon.com|https://www.amazon.sa|https://www.amazon.ae|https://www.amazon.eg|https://www.noon.com|https://www.jumia.com.eg|https://www.souq.com|https://www.namshi.com|https://www.aliexpress.com|https://www.alibaba.com|https://www.ebay.com|https://www.jarir.com|https://www.extra.com|https://www.sharafdg.com|https://www.xcite.com
4|وظائف|job_listings|400|false|https://www.linkedin.com/jobs|https://www.bayt.com|https://www.wuzzuf.net|https://www.forasna.com|https://www.mihnati.com|https://www.akhtaboot.com|https://www.glassdoor.com|https://www.indeed.com|https://www.naukrigulf.com|https://gulftalent.com|https://www.tanqeeb.com|https://hired.com
5|أكاديمي_جامعات|academic_research|150|false|https://www.cu.edu.eg|https://www.kau.edu.sa|https://www.kfupm.edu.sa|https://www.kaust.edu.sa|https://www.qu.edu.qa|https://www.aub.edu.lb|https://www.harvard.edu|https://www.mit.edu|https://www.stanford.edu|https://www.ox.ac.uk|https://arxiv.org|https://www.researchgate.net|https://pubmed.ncbi.nlm.nih.gov|https://www.sciencedirect.com|https://www.coursera.org|https://www.edx.org|https://www.udemy.com|https://www.edraak.org|https://khanacademy.org
6|تدريب_AI|ai_training_data|1000|false|https://stackoverflow.com|https://github.com|https://dev.to|https://medium.com|https://www.geeksforgeeks.org|https://realpython.com|https://towardsdatascience.com|https://www.kaggle.com|https://huggingface.co|https://ar.wikipedia.org|https://www.hindawi.org|https://www.adab.com|https://ar.wikisource.org|https://www.gutenberg.org|https://archive.org|https://www.noor-book.com
7|عقارات|real_estate|500|true|https://www.aqarmap.com.eg|https://www.propertyfinder.ae|https://www.bayut.com|https://www.dubizzle.com|https://www.opensooq.com|https://www.haraj.com.sa|https://sa.aqar.fm|https://www.waseet.net|https://www.olx.com.eg|https://www.avito.ma
8|صحة_طب|health_medical|200|false|https://www.webteb.com|https://www.altibbi.com|https://www.mayoclinic.org|https://www.healthline.com|https://www.webmd.com|https://www.nih.gov|https://www.who.int|https://medlineplus.gov|https://www.medicalnewstoday.com|https://www.nhs.uk
9|أخبار_رياضة_ترفيه|news_sports_entertainment|250|false|https://www.filgoal.com|https://www.kooora.com|https://www.yallakora.com|https://www.goal.com/ar|https://www.espn.com|https://www.nba.com|https://www.fifa.com|https://www.bbc.com/sport|https://www.elcinema.com|https://www.imdb.com|https://www.rottentomatoes.com
10|مالية_أسواق|finance_markets|200|false|https://www.argaam.com/ar|https://www.aleqt.com|https://www.mubasher.info|https://finance.yahoo.com|https://www.investing.com|https://www.bloomberg.com|https://coinmarketcap.com|https://www.tradingview.com
11|سفر_سياحة|travel_tourism|200|false|https://www.booking.com|https://www.tripadvisor.com|https://www.airbnb.com|https://www.expedia.com|https://www.almosafer.com/ar|https://www.skyscanner.com|https://www.agoda.com|https://www.arabiaweather.com/ar
12|تكنولوجيا_برمجة|tech_programming|300|false|https://stackoverflow.com|https://github.com|https://docs.python.org|https://developer.mozilla.org|https://www.w3schools.com|https://freecodecamp.org|https://www.geeksforgeeks.org|https://css-tricks.com|https://3arabtech.com|https://www.arageek.com|https://openai.com/blog|https://www.anthropic.com/news|https://huggingface.co/blog
"#;

const GLOBAL_CATALOG: &str = r#"
محركات البحث|https://www.google.com|https://www.bing.com|https://www.yahoo.com|https://duckduckgo.com|https://www.baidu.com|https://yandex.com
التواصل الاجتماعي|https://www.facebook.com|https://www.instagram.com|https://twitter.com|https://www.linkedin.com|https://www.tiktok.com|https://www.reddit.com|https://www.pinterest.com|https://www.snapchat.com|https://www.tumblr.com|https://www.quora.com
الفيديو والترفيه|https://www.youtube.com|https://www.netflix.com|https://www.twitch.tv|https://vimeo.com|https://www.dailymotion.com|https://www.hulu.com|https://www.disneyplus.com|https://www.imdb.com
الأخبار|https://www.bbc.com|https://www.cnn.com|https://www.nytimes.com|https://www.reuters.com|https://www.theguardian.com|https://www.aljazeera.com|https://www.foxnews.com|https://www.washingtonpost.com|https://www.nbcnews.com|https://www.bloomberg.com|https://www.forbes.com|https://www.bbc.co.uk|https://www.france24.com|https://www.dw.com|https://www.rt.com|https://apnews.com|https://www.npr.org|https://www.economist.com|https://time.com|https://www.newsweek.com
التكنولوجيا|https://www.apple.com|https://www.microsoft.com|https://www.amazon.com|https://www.meta.com|https://www.nvidia.com|https://www.samsung.com|https://www.intel.com|https://www.ibm.com|https://www.oracle.com|https://www.cisco.com|https://www.adobe.com|https://www.salesforce.com|https://github.com|https://stackoverflow.com|https://www.techcrunch.com|https://www.wired.com|https://arstechnica.com|https://www.theverge.com|https://www.engadget.com
التجارة الإلكترونية|https://www.amazon.com|https://www.ebay.com|https://www.alibaba.com|https://www.aliexpress.com|https://www.etsy.com|https://www.walmart.com|https://www.shopify.com|https://www.target.com|https://www.bestbuy.com|https://www.ikea.com
المالية والاقتصاد|https://www.paypal.com|https://www.stripe.com|https://www.visa.com|https://www.mastercard.com|https://finance.yahoo.com|https://www.investing.com|https://www.marketwatch.com|https://www.tradingview.com|https://coinmarketcap.com|https://www.binance.com
التعليم والمعرفة|https://www.wikipedia.org|https://www.coursera.org|https://www.udemy.com|https://www.edx.org|https://www.khanacademy.org|https://www.duolingo.com|https://scholar.google.com|https://www.researchgate.net|https://arxiv.org|https://www.academia.edu|https://www.britannica.com|https://www.wolframalpha.com
الصحة|https://www.webmd.com|https://www.mayoclinic.org|https://www.healthline.com|https://www.nih.gov|https://www.who.int|https://medlineplus.gov|https://www.medicalnewstoday.com
الحكومات والمنظمات|https://www.un.org|https://www.whitehouse.gov|https://www.gov.uk|https://europa.eu|https://www.nasa.gov|https://www.worldbank.org|https://www.imf.org|https://www.ieee.org
الترفيه والرياضة|https://www.espn.com|https://www.nba.com|https://www.fifa.com|https://www.nfl.com|https://www.olympic.org|https://www.cricinfo.com|https://www.transfermarkt.com
السفر|https://www.booking.com|https://www.tripadvisor.com|https://www.airbnb.com|https://www.expedia.com|https://www.kayak.com|https://www.skyscanner.com|https://www.hotels.com
البريد والتواصل|https://mail.google.com|https://outlook.live.com|https://www.whatsapp.com|https://telegram.org|https://discord.com|https://slack.com|https://zoom.us
الذكاء الاصطناعي|https://openai.com|https://www.anthropic.com|https://gemini.google.com|https://www.midjourney.com|https://huggingface.co|https://stability.ai
السحابة والاستضافة|https://aws.amazon.com|https://cloud.google.com|https://azure.microsoft.com|https://www.cloudflare.com|https://www.digitalocean.com|https://www.godaddy.com|https://www.namecheap.com
المواقع العربية|https://www.aljazeera.net|https://arabic.cnn.com|https://www.bbc.com/arabic|https://www.alarabiya.net|https://www.skynewsarabia.com|https://www.youm7.com|https://www.masrawy.com|https://www.aawsat.com|https://www.albawabhnews.com|https://www.hespress.com
"#;

const ARABIC_CATALOG: &str = r#"
أخبار مصر|https://www.youm7.com/|https://www.masrawy.com/|https://www.elwatannews.com/|https://www.almasryalyoum.com/|https://www.akhbarelyom.com/|https://www.dostor.org/|https://www.vetogate.com/|https://www.filgoal.com/|https://www.shorouknews.com/|https://www.mobtada.com/|https://www.e3lam.com/|https://www.elyom.com/|https://aqarmap.com.eg/ar/|https://www.cairo24.com/|https://gate.ahram.org.eg/
أخبار خليجية|https://www.alriyadh.com/|https://www.okaz.com.sa/|https://www.sabq.org/|https://www.argaam.com/ar/|https://www.aleqt.com/|https://www.alarabiya.net/|https://www.skynewsarabia.com/|https://www.khaleejtimes.com/|https://www.gulfnews.com/|https://www.alkhaleej.ae/|https://www.albayan.ae/|https://www.emaratalyoum.com/|https://www.alwatan.com.sa/|https://www.alyaum.com/|https://www.kuna.net.kw/|https://www.qna.org.qa/|https://wam.ae/ar/|https://www.timesofoman.com/
أخبار عربية عامة|https://www.aljazeera.net/|https://arabic.cnn.com/|https://www.bbc.com/arabic|https://arabic.rt.com/|https://www.france24.com/ar/|https://www.dw.com/ar/|https://www.euronews.com/ar|https://www.aawsat.com/|https://www.alhurra.com/|https://www.independentarabia.com/|https://www.arabi21.com/|https://www.raialyoum.com/|https://www.alquds.co.uk/|https://www.huffpostarabi.com/|https://www.almayadeen.net/
المغرب والشمال الأفريقي|https://www.hespress.com/|https://www.h24info.ma/|https://www.goud.ma/|https://www.yabiladi.com/|https://www.echoroukonline.com/|https://www.ennaharonline.com/|https://www.el-khabar.com/|https://www.tsa-algerie.com/ar/|https://www.babnet.net/|https://www.tunisienumerique.com/ar/
الشام والعراق|https://www.enabbaladi.net/|https://www.zamanalwsl.net/|https://www.syria.tv/|https://www.orient-news.net/|https://www.alaraby.co.uk/|https://www.alsumaria.tv/|https://www.rudawarabia.net/|https://www.shafaq.com/ar/|https://www.almadapaper.net/
معرفة وموسوعات|https://ar.wikipedia.org/|https://mawdoo3.com/|https://www.almaany.com/|https://www.marefa.org/|https://www.mosoah.com/|https://ar.wiktionary.org/|https://ar.wikiquote.org/|https://ar.wikisource.org/|https://ar.wikibooks.org/|https://www.alukah.net/
دين وإسلام|https://islamqa.info/ar|https://www.islamweb.net/ar/|https://quran.com/ar|https://www.sunnah.com/|https://www.noor-book.com/|https://www.alukah.net/|https://www.islamhouse.com/ar/|https://www.al-eman.com/|https://dorar.net/|https://www.saaid.net/
تكنولوجيا|https://www.tech-wd.com/|https://www.arabnet.me/ar/|https://www.arageek.com/|https://www.arabicgeek.com/|https://www.dotmsr.com/|https://wamda.com/ar|https://www.itp.net/ar|https://3arabtech.com/
اقتصاد ومال|https://www.argaam.com/ar/|https://www.aleqt.com/|https://www.mubasher.info/|https://www.arabfinance.com/|https://www.amwalalghad.com/|https://www.zawya.com/ar/|https://www.aliqtisadi.com/
رياضة|https://www.filgoal.com/|https://www.kooora.com/|https://www.yallakora.com/|https://www.koora.com/|https://www.goal.com/ar|https://www.bein.com/ar/|https://www.sport360.com/ar/|https://www.alarabiya.net/sports|https://www.riyadi.net/
ترفيه وسينما|https://www.elcinema.com/|https://www.alcinema.net/|https://www.anghami.com/|https://ar.4arab.com/|https://www.arabianbusiness.com/ar/
صحة وطب|https://www.webteb.com/|https://www.altibbi.com/|https://www.mayoclinic.org/ar|https://www.healthline.com/ar|https://www.sehetna.com/|https://www.alnafsy.com/|https://www.wikihow.com/ar/
تعليم وثقافة|https://www.noor-book.com/|https://www.goodreads.com/ar|https://www.hindawi.org/|https://www.abjjad.com/|https://www.neelwafurat.com/|https://www.coursera.org/ar/|https://www.edraak.org/|https://www.rwaq.org/
سفر وسياحة|https://www.almosafer.com/ar|https://www.booking.com/|https://www.tripadvisor.com.eg/|https://www.arabiaweather.com/ar|https://www.egyegy.com/
منتديات ومجتمعات|https://www.arabsgate.com/|https://www.fatakat.com/|https://www.hawaaworld.com/|https://www.adab.com/|https://www.alfaseeh.com/|https://www.sh3ir.com/|https://www.qaswah.com/
حكومات عربية|https://www.gov.sa/|https://www.uae.gov.ae/ar/|https://www.egypt.gov.eg/|https://www.jordan.gov.jo/|https://www.bahrain.bh/|https://www.maroc.ma/
"#;

const UNIVERSITY_CATALOG: &str = r#"
USA - Ivy League & Top|https://www.harvard.edu|https://www.mit.edu|https://www.stanford.edu|https://www.yale.edu|https://www.princeton.edu|https://www.columbia.edu|https://www.upenn.edu|https://www.cornell.edu|https://www.dartmouth.edu|https://www.brown.edu|https://www.caltech.edu|https://www.uchicago.edu|https://www.duke.edu|https://www.northwestern.edu|https://www.jhu.edu|https://www.vanderbilt.edu|https://www.rice.edu|https://www.wustl.edu|https://www.emory.edu|https://www.georgetown.edu
USA - Public Universities|https://www.ucla.edu|https://www.berkeley.edu|https://www.umich.edu|https://www.unc.edu|https://www.virginia.edu|https://www.gatech.edu|https://www.ufl.edu|https://www.osu.edu|https://www.psu.edu|https://www.wisc.edu|https://www.umn.edu|https://www.purdue.edu|https://www.tamu.edu|https://www.utexas.edu|https://www.ucdavis.edu|https://www.ucsb.edu|https://www.ucsd.edu|https://www.ucsf.edu|https://www.uiuc.edu|https://www.indiana.edu|https://www.colorado.edu|https://www.arizona.edu|https://www.asu.edu|https://www.usc.edu|https://www.bu.edu|https://www.northeastern.edu|https://www.nyu.edu|https://www.rutgers.edu|https://www.umd.edu|https://www.gwu.edu|https://www.american.edu|https://www.howard.edu|https://www.tulane.edu|https://www.lsu.edu|https://www.miami.edu|https://www.fsu.edu|https://www.ucf.edu|https://www.usf.edu|https://www.utah.edu|https://www.byu.edu|https://www.unlv.edu|https://www.oregonstate.edu|https://www.uoregon.edu|https://www.washington.edu|https://www.wsu.edu|https://www.uw.edu
Canada|https://www.utoronto.ca|https://www.mcgill.ca|https://www.ubc.ca|https://www.ualberta.ca|https://www.uwaterloo.ca|https://www.queensu.ca|https://www.mcmaster.ca|https://www.uottawa.ca|https://www.western.ca|https://www.dal.ca|https://www.usask.ca|https://www.umanitoba.ca|https://www.ucalgary.ca|https://www.sfu.ca|https://www.concordia.ca|https://www.uqam.ca|https://www.yorku.ca|https://www.ryerson.ca|https://www.carleton.ca|https://www.unb.ca
UK|https://www.ox.ac.uk|https://www.cam.ac.uk|https://www.imperial.ac.uk|https://www.ucl.ac.uk|https://www.lse.ac.uk|https://www.kcl.ac.uk|https://www.ed.ac.uk|https://www.manchester.ac.uk|https://www.bristol.ac.uk|https://www.warwick.ac.uk|https://www.leeds.ac.uk|https://www.birmingham.ac.uk|https://www.sheffield.ac.uk|https://www.nottingham.ac.uk|https://www.southampton.ac.uk|https://www.liverpool.ac.uk|https://www.exeter.ac.uk|https://www.durham.ac.uk|https://www.glasgow.ac.uk|https://www.qub.ac.uk|https://www.bath.ac.uk|https://www.york.ac.uk|https://www.lancaster.ac.uk|https://www.cardiff.ac.uk|https://www.swansea.ac.uk|https://www.dundee.ac.uk
Germany|https://www.lmu.de|https://www.tum.de|https://www.uni-heidelberg.de|https://www.fu-berlin.de|https://www.hu-berlin.de|https://www.tu-berlin.de|https://www.uni-bonn.de|https://www.uni-hamburg.de|https://www.uni-frankfurt.de|https://www.uni-koeln.de|https://www.rwth-aachen.de|https://www.kit.edu|https://www.uni-stuttgart.de|https://www.uni-freiburg.de|https://www.uni-goettingen.de|https://www.uni-tuebingen.de
France|https://www.sorbonne-universite.fr|https://www.polytechnique.edu|https://www.ens.fr|https://www.sciences-po.fr|https://www.centrale-supelec.fr|https://www.univ-lyon1.fr|https://www.univ-grenoble-alpes.fr|https://www.univ-bordeaux.fr|https://www.univ-strasbourg.fr|https://www.univ-toulouse.fr
Netherlands & Belgium|https://www.leiden.edu|https://www.uva.nl|https://www.tudelft.nl|https://www.uu.nl|https://www.rug.nl|https://www.kuleuven.be|https://www.ugent.be|https://www.uclouvain.be
Switzerland & Austria|https://www.ethz.ch|https://www.epfl.ch|https://www.uzh.ch|https://www.unige.ch|https://www.univie.ac.at|https://www.tuwien.at
Scandinavia|https://www.ku.dk|https://www.dtu.dk|https://www.lu.se|https://www.su.se|https://www.kth.se|https://www.chalmers.se|https://www.uio.no|https://www.ntnu.no|https://www.helsinki.fi|https://www.aalto.fi
Southern Europe|https://www.unimi.it|https://www.unibo.it|https://www.uniroma1.it|https://www.polimi.it|https://www.ucm.es|https://www.uab.es|https://www.upf.edu|https://www.ulisboa.pt|https://www.uoa.gr|https://www.auth.gr
Eastern Europe|https://www.msu.ru|https://www.spbu.ru|https://www.mipt.ru|https://www.hse.ru|https://www.cuni.cz|https://www.uw.edu.pl|https://www.elte.hu|https://www.unibuc.ro|https://www.uni-sofia.bg|https://www.bg.ac.rs
China|https://www.tsinghua.edu.cn|https://www.pku.edu.cn|https://www.fudan.edu.cn|https://www.sjtu.edu.cn|https://www.zju.edu.cn|https://www.ustc.edu.cn|https://www.nju.edu.cn|https://www.whu.edu.cn|https://www.sysu.edu.cn|https://www.hit.edu.cn
Japan|https://www.u-tokyo.ac.jp|https://www.kyoto-u.ac.jp|https://www.osaka-u.ac.jp|https://www.tohoku.ac.jp|https://www.nagoya-u.ac.jp|https://www.titech.ac.jp|https://www.waseda.jp|https://www.keio.ac.jp|https://www.kobe-u.ac.jp|https://www.hiroshima-u.ac.jp
South Korea|https://www.snu.ac.kr|https://www.yonsei.ac.kr|https://www.korea.ac.kr|https://www.kaist.ac.kr|https://www.postech.ac.kr|https://www.skku.edu|https://www.hanyang.ac.kr|https://www.ewha.ac.kr
India|https://www.iitb.ac.in|https://www.iitd.ac.in|https://www.iitm.ac.in|https://www.iitkgp.ac.in|https://www.iisc.ac.in|https://www.du.ac.in|https://www.jnu.ac.in|https://www.vit.ac.in|https://www.manipal.edu|https://www.amity.edu
Singapore & Hong Kong|https://www.nus.edu.sg|https://www.ntu.edu.sg|https://www.smu.edu.sg|https://www.hku.hk|https://www.cuhk.edu.hk|https://www.ust.hk|https://www.polyu.edu.hk|https://www.cityu.edu.hk
Middle East (Arab)|https://www.kaust.edu.sa|https://www.ksu.edu.sa|https://www.kau.edu.sa|https://www.kfupm.edu.sa|https://www.uaeu.ac.ae|https://www.aus.edu|https://www.qu.edu.qa|https://www.hbku.edu.qa|https://www.ju.edu.jo|https://www.aub.edu.lb|https://www.lau.edu.lb|https://www.ku.edu.kw|https://www.squ.edu.om|https://www.uob.edu.bh
Egypt|https://www.cu.edu.eg|https://www.alexu.edu.eg|https://www.asu.edu.eg|https://www.mans.edu.eg|https://www.zu.edu.eg|https://www.svu.edu.eg|https://www.bsu.edu.eg|https://www.minia.edu.eg|https://www.sohag-univ.edu.eg|https://www.aun.edu.eg|https://www.aucegypt.edu|https://www.guc.edu.eg|https://www.fue.edu.eg|https://www.msa.edu.eg|https://www.nu.edu.eg
North Africa|https://www.univ-alger.dz|https://www.univ-oran.dz|https://www.usthb.dz|https://www.um5.ac.ma|https://www.usmba.ac.ma|https://www.univ-tunis.tn|https://www.enit.rnu.tn|https://www.uol.edu.ly
Turkey & Iran|https://www.metu.edu.tr|https://www.boun.edu.tr|https://www.itu.edu.tr|https://www.bilkent.edu.tr|https://www.sabanciuniv.edu|https://www.hacettepe.edu.tr|https://www.sharif.edu|https://www.ut.ac.ir|https://www.modares.ac.ir|https://www.iust.ac.ir
Pakistan & Bangladesh|https://www.pu.edu.pk|https://www.uet.edu.pk|https://www.lums.edu.pk|https://www.nust.edu.pk|https://www.comsats.edu.pk|https://www.iba.edu.pk|https://www.du.ac.bd|https://www.buet.ac.bd|https://www.bracu.ac.bd
Southeast Asia|https://www.ui.ac.id|https://www.itb.ac.id|https://www.ugm.ac.id|https://www.chula.ac.th|https://www.mahidol.ac.th|https://www.um.edu.my|https://www.utm.edu.my|https://www.up.edu.ph|https://www.vnu.edu.vn|https://www.hust.edu.vn
Australia & New Zealand|https://www.anu.edu.au|https://www.unimelb.edu.au|https://www.sydney.edu.au|https://www.unsw.edu.au|https://www.uq.edu.au|https://www.monash.edu|https://www.uwa.edu.au|https://www.adelaide.edu.au|https://www.auckland.ac.nz|https://www.otago.ac.nz|https://www.vuw.ac.nz
Latin America|https://www.usp.br|https://www.unicamp.br|https://www.ufrj.br|https://www.unam.mx|https://www.tec.mx|https://www.uba.ar|https://www.uchile.cl|https://www.unal.edu.co|https://www.uniandes.edu.co|https://www.pucp.edu.pe
Sub-Saharan Africa|https://www.uct.ac.za|https://www.wits.ac.za|https://www.up.ac.za|https://www.sun.ac.za|https://www.uonbi.ac.ke|https://www.ug.edu.gh|https://www.unilag.edu.ng|https://www.ui.edu.ng|https://www.udsm.ac.tz|https://www.muk.ac.ug
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrawlerKind {
    Advanced,
    Arabic,
    Universities,
    Global,
    SearchEngine,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConflictPolicy {
    /// TiDB `INSERT IGNORE` / duplicate URL is ignored.
    TidbInsertIgnore,
    /// CockroachDB `ON CONFLICT (url) DO NOTHING`.
    CockroachDoNothing,
    /// Existing durable search store's `index_document` semantics.
    SearchStore,
}

#[derive(Clone, Debug)]
pub struct CrawlerConfig {
    pub kind: CrawlerKind,
    pub max_depth: usize,
    pub concurrency: usize,
    pub max_pages: usize,
    pub max_per_domain: usize,
    pub batch_size: usize,
    pub delay: Duration,
    pub max_links_per_page: usize,
    pub max_response_bytes: Option<usize>,
    pub respect_robots: bool,
    pub same_domain_only: bool,
    pub deduplicate_titles: bool,
    pub retry_limit: usize,
    pub request_timeout: Duration,
    pub conflict_policy: ConflictPolicy,
}

impl CrawlerConfig {
    /// Limits and write policy used by `advanced_async_crawler.py`.
    pub fn advanced() -> Self {
        Self {
            kind: CrawlerKind::Advanced,
            max_depth: 3,
            concurrency: 20,
            max_pages: 500_000,
            max_per_domain: 200,
            batch_size: 100,
            delay: Duration::from_millis(200),
            max_links_per_page: 50,
            max_response_bytes: Some(DEFAULT_MAX_RESPONSE_BYTES),
            respect_robots: false,
            same_domain_only: false,
            deduplicate_titles: true,
            retry_limit: 0,
            request_timeout: Duration::from_secs(20),
            conflict_policy: ConflictPolicy::TidbInsertIgnore,
        }
    }

    /// Limits and write policy used by `crawler_arabic.py`.
    pub fn arabic() -> Self {
        Self {
            kind: CrawlerKind::Arabic,
            max_depth: 4,
            concurrency: 15,
            max_pages: 100_000,
            max_per_domain: 200,
            batch_size: 50,
            delay: Duration::from_millis(200),
            max_links_per_page: 40,
            max_response_bytes: Some(DEFAULT_MAX_RESPONSE_BYTES),
            respect_robots: false,
            same_domain_only: false,
            deduplicate_titles: true,
            retry_limit: 0,
            request_timeout: Duration::from_secs(20),
            conflict_policy: ConflictPolicy::CockroachDoNothing,
        }
    }

    /// Limits and write policy used by `cloud_crawler_runner.py`.
    pub fn universities() -> Self {
        Self {
            kind: CrawlerKind::Universities,
            max_depth: 3,
            concurrency: 15,
            max_pages: 200_000,
            max_per_domain: 150,
            batch_size: 50,
            delay: Duration::from_millis(250),
            max_links_per_page: 40,
            max_response_bytes: Some(DEFAULT_MAX_RESPONSE_BYTES),
            respect_robots: false,
            same_domain_only: false,
            deduplicate_titles: true,
            retry_limit: 0,
            request_timeout: Duration::from_secs(20),
            conflict_policy: ConflictPolicy::CockroachDoNothing,
        }
    }

    /// Defaults used by `simple_crawler.py`; the Python constructor can
    /// override `max_pages` and `concurrency` before starting a crawl.
    pub fn global() -> Self {
        Self {
            kind: CrawlerKind::Global,
            max_depth: 2,
            concurrency: 15,
            max_pages: 500,
            max_per_domain: 5,
            batch_size: 10,
            delay: Duration::from_millis(300),
            max_links_per_page: 8,
            max_response_bytes: Some(DEFAULT_MAX_RESPONSE_BYTES),
            respect_robots: false,
            same_domain_only: true,
            deduplicate_titles: true,
            retry_limit: 0,
            request_timeout: Duration::from_secs(15),
            conflict_policy: ConflictPolicy::TidbInsertIgnore,
        }
    }

    /// Queue-backed `search_engine/crawler.py` behavior.
    pub fn search_engine() -> Self {
        Self {
            kind: CrawlerKind::SearchEngine,
            max_depth: 2,
            concurrency: 1,
            max_pages: 100,
            max_per_domain: usize::MAX,
            batch_size: 1,
            delay: Duration::from_secs(1),
            max_links_per_page: 100,
            max_response_bytes: Some(2_000_000),
            respect_robots: true,
            same_domain_only: true,
            deduplicate_titles: false,
            retry_limit: 0,
            request_timeout: Duration::from_secs(15),
            conflict_policy: ConflictPolicy::SearchStore,
        }
    }

    pub fn with_limits(mut self, max_pages: usize, concurrency: usize) -> Self {
        self.max_pages = max_pages;
        self.concurrency = concurrency.max(1);
        self
    }

    pub fn with_retry_limit(mut self, retry_limit: usize) -> Self {
        self.retry_limit = retry_limit;
        self
    }

    pub fn with_body_limit(mut self, max_bytes: usize) -> Self {
        self.max_response_bytes = Some(max_bytes);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Seed {
    pub url: String,
    pub category: String,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub depth: usize,
    /// Optional per-category domain cap from the source target catalog.
    #[serde(default)]
    pub domain_limit: Option<usize>,
    /// Advanced crawler category flag controlling price extraction.
    #[serde(default)]
    pub extract_prices: Option<bool>,
}

impl Seed {
    pub fn new(url: impl Into<String>, category: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            category: category.into(),
            purpose: String::new(),
            depth: 0,
            domain_limit: None,
            extract_prices: None,
        }
    }
}

/// Return the source crawler's complete hard-coded seed list in source order.
/// `SearchEngine` has no hard-coded catalog; its seeds come from the durable
/// cloud queue populated by the host application.
pub fn seed_catalog(kind: CrawlerKind) -> Vec<Seed> {
    match kind {
        CrawlerKind::Advanced => ADVANCED_CATALOG
            .lines()
            .filter(|line| !line.trim().is_empty())
            .flat_map(|line| {
                let mut fields = line.split('|');
                let _priority = fields.next();
                let category = fields.next().unwrap_or_default();
                let purpose = fields.next().unwrap_or_default();
                let domain_limit = fields.next().and_then(|value| value.parse().ok());
                let extract_prices = fields.next().map(|value| value == "true");
                fields
                    .map(|url| Seed {
                        url: url.to_owned(),
                        category: category.to_owned(),
                        purpose: purpose.to_owned(),
                        depth: 0,
                        domain_limit,
                        extract_prices,
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
        CrawlerKind::Arabic => parse_category_catalog(ARABIC_CATALOG),
        CrawlerKind::Universities => parse_category_catalog(UNIVERSITY_CATALOG),
        CrawlerKind::Global => parse_category_catalog(GLOBAL_CATALOG),
        CrawlerKind::SearchEngine => Vec::new(),
    }
}

fn parse_category_catalog(catalog: &str) -> Vec<Seed> {
    catalog
        .lines()
        .filter(|line| !line.trim().is_empty())
        .flat_map(|line| {
            let mut fields = line.split('|');
            let category = fields.next().unwrap_or_default();
            fields
                .map(|url| Seed::new(url, category))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Price {
    pub value: String,
    pub currency: String,
}

/// Shared record shape for the legacy table schemas and current search store.
/// A writer maps only fields used by its schema and must apply the selected
/// [`ConflictPolicy`] transactionally for each supplied batch.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CrawlPage {
    pub url: String,
    pub domain: String,
    pub category: String,
    pub country: String,
    pub purpose: String,
    pub title: String,
    pub description: String,
    pub og_description: String,
    pub keywords: String,
    pub og_image: String,
    pub canonical: String,
    pub content: String,
    pub language: String,
    pub direction: String,
    pub html_lang: String,
    pub author: String,
    pub word_count: usize,
    pub is_arabic: bool,
    pub status_code: u16,
    pub depth: usize,
    pub crawler: String,
    pub crawled_at: f64,
    pub prices: Vec<Price>,
    /// Outbound links recorded at parse time and persisted inside the source
    /// metadata so the PageRank maintenance command can rebuild the link graph.
    pub links: Vec<String>,
    /// Favicon URL discovered while crawling (a `<link rel="icon">` tag, or
    /// the default `{scheme}://{host}/favicon.ico` when none was declared).
    /// Stored verbatim in the document metadata so the search frontend can
    /// display it without re-parsing HTML or calling external services.
    pub favicon: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CrawlStats {
    pub processed: usize,
    pub saved: usize,
    pub failed: usize,
    pub skipped: usize,
    pub arabic: usize,
    pub prices_found: usize,
    pub by_purpose: HashMap<String, usize>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CrawlReport {
    pub stats: CrawlStats,
    pub domains: usize,
}

pub type CloudWriteFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, String>> + Send + 'a>>;

/// Cloud-write boundary for the legacy TiDB/CockroachDB tables or search
/// store. The host adapter owns schema provisioning and reuses the configured
/// cloud TLS connection. TiDB ignores duplicate keys; CockroachDB does nothing
/// on URL conflicts. `Ok` returns the number of records actually inserted.
pub trait CloudPageWriter: Sync {
    fn write_batch<'a>(
        &'a self,
        kind: CrawlerKind,
        policy: ConflictPolicy,
        pages: &'a [CrawlPage],
    ) -> CloudWriteFuture<'a>;
}

#[derive(Clone, Debug)]
pub struct RobotsRules {
    rules: Vec<(bool, String)>,
}

/// One robots.txt group: the user-agents it covers and its allow/disallow rules.
type RuleGroup = (Vec<String>, Vec<(bool, String)>);

impl RobotsRules {
    pub fn parse(text: &str, user_agent: &str) -> Self {
        let wanted = user_agent.to_ascii_lowercase();
        let mut groups: Vec<RuleGroup> = Vec::new();
        let mut agents = Vec::new();
        let mut rules = Vec::new();
        let mut saw_rule = false;
        for raw in text.lines() {
            let line = raw.split('#').next().unwrap_or("").trim();
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim();
            match name.as_str() {
                "user-agent" => {
                    if saw_rule && !agents.is_empty() {
                        groups.push((std::mem::take(&mut agents), std::mem::take(&mut rules)));
                        saw_rule = false;
                    }
                    agents.push(value.to_ascii_lowercase());
                }
                "allow" | "disallow" => {
                    saw_rule = true;
                    if !value.is_empty() {
                        rules.push((name == "allow", value.to_owned()));
                    }
                }
                _ => {}
            }
        }
        if !agents.is_empty() {
            groups.push((agents, rules));
        }

        let mut best_len = None;
        let mut selected = Vec::new();
        for (agents, candidate_rules) in groups {
            let match_len = agents
                .iter()
                .filter(|agent| wanted.contains(agent.as_str()))
                .map(|agent| agent.len())
                .max()
                .or_else(|| agents.iter().any(|a| a == "*").then_some(0));
            if let Some(length) = match_len {
                if best_len.is_none_or(|current| length > current) {
                    selected = candidate_rules;
                    best_len = Some(length);
                } else if best_len == Some(length) {
                    selected.extend(candidate_rules);
                }
            }
        }
        Self { rules: selected }
    }

    pub fn allows(&self, url: &Url) -> bool {
        let query = url.query().map(|q| format!("?{q}")).unwrap_or_default();
        let target = format!("{}{query}", url.path());
        let mut selected: Option<(usize, bool)> = None;
        for (allow, pattern) in &self.rules {
            let (pattern, anchored) = pattern
                .strip_suffix('$')
                .map_or((pattern.as_str(), false), |p| (p, true));
            let mut regex = String::from("^");
            let mut start = 0;
            for (index, ch) in pattern.char_indices() {
                if ch == '*' {
                    regex.push_str(&regex::escape(&pattern[start..index]));
                    regex.push_str(".*");
                    start = index + ch.len_utf8();
                }
            }
            regex.push_str(&regex::escape(&pattern[start..]));
            if anchored {
                regex.push('$');
            }
            if Regex::new(&regex)
                .map(|re| re.is_match(&target))
                .unwrap_or(false)
            {
                let specificity = pattern.chars().filter(|ch| *ch != '*').count();
                if selected.is_none_or(|(old, old_allow)| {
                    specificity > old || (specificity == old && *allow && !old_allow)
                }) {
                    selected = Some((specificity, *allow));
                }
            }
        }
        selected.map(|(_, allow)| allow).unwrap_or(true)
    }
}

struct ParsedPage {
    record: CrawlPage,
    links: Vec<String>,
    noindex: bool,
}

#[derive(Clone, Debug)]
pub struct FetchedPage {
    pub seed: Seed,
    pub page: CrawlPage,
    pub final_url: Url,
    pub discovered_urls: Vec<String>,
    pub status_code: u16,
    pub noindex: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FetchError {
    InvalidUrl(String),
    UnsupportedDomain(String),
    RobotsDisallowed(String),
    HttpStatus(u16),
    NonHtml,
    BodyTooLarge(usize),
    Request(String),
    Parse(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(error) => write!(formatter, "invalid URL: {error}"),
            Self::UnsupportedDomain(host) => write!(formatter, "unsupported domain: {host}"),
            Self::RobotsDisallowed(url) => write!(formatter, "robots.txt disallows {url}"),
            Self::HttpStatus(status) => write!(formatter, "HTTP {status}"),
            Self::NonHtml => formatter.write_str("non-HTML response"),
            Self::BodyTooLarge(limit) => write!(formatter, "response exceeds {limit} byte limit"),
            Self::Request(error) | Self::Parse(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for FetchError {}

/// Reusable async HTTP/parser client. Optional domain allow-lists accept an
/// exact hostname and its subdomains. Profile defaults independently apply
/// their source same-domain link rules. Transport errors and 408/425/429/5xx
/// responses are retryable up to `CrawlerConfig::retry_limit`; profile
/// defaults remain zero retries, like the source crawler implementations.
pub struct CrawlerFetcher {
    config: CrawlerConfig,
    client: reqwest::Client,
    supported_domains: Option<HashSet<String>>,
    robots_cache: std::sync::Mutex<HashMap<String, RobotsRules>>,
    host_gates: std::sync::Mutex<HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>,
    last_request: std::sync::Mutex<HashMap<String, tokio::time::Instant>>,
}

impl CrawlerFetcher {
    pub fn new(config: CrawlerConfig) -> Result<Self, String> {
        let agent = match config.kind {
            CrawlerKind::SearchEngine => USER_AGENT,
            CrawlerKind::Universities => {
                "Mozilla/5.0 (compatible; UniBot/2.0; +https://example.com/bot)"
            }
            _ => LEGACY_BROWSER_AGENT,
        };
        let client = reqwest::Client::builder()
            .user_agent(agent)
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            config,
            client,
            supported_domains: None,
            robots_cache: std::sync::Mutex::new(HashMap::new()),
            host_gates: std::sync::Mutex::new(HashMap::new()),
            last_request: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub fn with_supported_domains(mut self, domains: impl IntoIterator<Item = String>) -> Self {
        self.supported_domains = Some(
            domains
                .into_iter()
                .map(|domain| {
                    domain
                        .trim()
                        .trim_start_matches("*.")
                        .trim_end_matches('.')
                        .to_ascii_lowercase()
                })
                .filter(|domain| !domain.is_empty())
                .collect(),
        );
        self
    }

    pub fn is_supported_url(&self, url: &Url) -> bool {
        let Some(domains) = &self.supported_domains else {
            return true;
        };
        let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
            return false;
        };
        domains
            .iter()
            .any(|domain| host == domain.as_str() || host.ends_with(&format!(".{domain}")))
    }

    /// Fetch, validate, decode, and parse one URL. `page` is ready to persist;
    /// `discovered_urls` are already resolved and normalized according to the
    /// selected source profile.
    pub async fn fetch_page(&self, seed: Seed) -> Result<FetchedPage, FetchError> {
        let initial_url =
            Url::parse(&seed.url).map_err(|error| FetchError::InvalidUrl(error.to_string()))?;
        if !matches!(initial_url.scheme(), "http" | "https") {
            return Err(FetchError::InvalidUrl(
                "only HTTP(S) URLs are crawled".into(),
            ));
        }
        if !self.is_supported_url(&initial_url) {
            return Err(FetchError::UnsupportedDomain(
                initial_url.host_str().unwrap_or("").to_owned(),
            ));
        }
        if self.config.respect_robots && !self.robots_allowed(&initial_url).await {
            return Err(FetchError::RobotsDisallowed(seed.url.clone()));
        }

        let host = domain_key(&initial_url);
        let host_guard = if self.config.kind == CrawlerKind::SearchEngine {
            let gate = {
                let mut gates = self
                    .host_gates
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                gates
                    .entry(host.clone())
                    .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                    .clone()
            };
            Some(gate.lock_owned().await)
        } else {
            None
        };

        if self.config.kind == CrawlerKind::SearchEngine {
            let last = self
                .last_request
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&host)
                .copied();
            if let Some(last) = last {
                let wait = self.config.delay.saturating_sub(last.elapsed());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
        }

        let max_bytes = self
            .config
            .max_response_bytes
            .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES);
        for attempt in 0..=self.config.retry_limit {
            let mut response = match self.client.get(initial_url.clone()).send().await {
                Ok(response) => response,
                Err(_error) if attempt < self.config.retry_limit => {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                Err(error) => return Err(FetchError::Request(error.to_string())),
            };
            if self.config.kind == CrawlerKind::SearchEngine {
                self.last_request
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(host.clone(), tokio::time::Instant::now());
            }
            let status = response.status().as_u16();
            let final_url = response.url().clone();
            if !self.is_supported_url(&final_url) {
                return Err(FetchError::UnsupportedDomain(
                    final_url.host_str().unwrap_or("").to_owned(),
                ));
            }
            if self.config.respect_robots && !self.robots_allowed(&final_url).await {
                return Err(FetchError::RobotsDisallowed(final_url.to_string()));
            }
            if !accepted_status(self.config.kind, status) {
                if attempt < self.config.retry_limit && retryable_status(status) {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                return Err(FetchError::HttpStatus(status));
            }
            if self.config.kind != CrawlerKind::Global
                && !response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_ascii_lowercase()
                    .contains("text/html")
            {
                return Err(FetchError::NonHtml);
            }
            let html = match read_response_body(&mut response, Some(max_bytes)).await {
                Ok(html) => html,
                Err(FetchError::Request(_)) if attempt < self.config.retry_limit => {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let parse_url = if self.config.kind == CrawlerKind::SearchEngine {
                &final_url
            } else {
                &initial_url
            };
            let parsed = parse_page(self.config.kind, &html, parse_url, &seed, status)
                .ok_or_else(|| FetchError::Parse("could not parse HTML page".into()))?;
            let discovered_urls = if parsed.noindex {
                Vec::new()
            } else {
                parsed
                    .links
                    .into_iter()
                    .filter(|link| {
                        Url::parse(link)
                            .ok()
                            .is_some_and(|url| self.is_supported_url(&url))
                    })
                    .collect()
            };
            drop(host_guard);
            return Ok(FetchedPage {
                seed,
                page: parsed.record,
                final_url,
                discovered_urls,
                status_code: status,
                noindex: parsed.noindex,
            });
        }
        unreachable!("retry loop always returns or continues")
    }

    async fn robots_allowed(&self, url: &Url) -> bool {
        let origin = url.origin().ascii_serialization();
        let cached = self
            .robots_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&origin)
            .cloned();
        let rules = if let Some(rules) = cached {
            rules
        } else {
            let text = match self.client.get(format!("{origin}/robots.txt")).send().await {
                Ok(mut response) if response.status().as_u16() == 200 => {
                    match read_response_body(&mut response, Some(512 * 1024)).await {
                        Ok(text) => text,
                        Err(error) => {
                            eprintln!("Could not read robots.txt for {origin}: {error}");
                            String::new()
                        }
                    }
                }
                Ok(response) => {
                    if response.status().as_u16() != 404 {
                        eprintln!(
                            "robots.txt request for {origin} returned HTTP {}",
                            response.status()
                        );
                    }
                    String::new()
                }
                Err(error) => {
                    eprintln!("Could not fetch robots.txt for {origin}: {error}");
                    String::new()
                }
            };
            let rules = RobotsRules::parse(&text, USER_AGENT);
            self.robots_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(origin, rules.clone());
            rules
        };
        rules.allows(url)
    }
}

/// Crawl the supplied seed frontier using the selected legacy profile.
/// Failed cloud batches are logged and counted as failed; as in the Python
/// crawlers, a failed flush is not automatically retried.
pub async fn crawl<W: CloudPageWriter>(
    config: CrawlerConfig,
    seeds: impl IntoIterator<Item = Seed>,
    writer: &W,
) -> Result<CrawlReport, String> {
    let concurrency = config.concurrency.max(1);
    let mut frontier: VecDeque<Seed> = seeds.into_iter().collect();
    let mut visited = HashSet::new();
    let mut domain_counts: HashMap<String, usize> = HashMap::new();
    let mut title_seen = HashSet::new();
    let mut buffered = Vec::with_capacity(config.batch_size.max(1));
    let mut stats = CrawlStats::default();
    let fetcher = CrawlerFetcher::new(config.clone())?;

    while !frontier.is_empty() && stats.processed < config.max_pages {
        let mut batch = Vec::with_capacity(concurrency);
        while batch.len() < concurrency && stats.processed < config.max_pages {
            let Some(seed) = frontier.pop_front() else {
                break;
            };
            let Ok(parsed) = Url::parse(&seed.url) else {
                stats.failed += 1;
                stats.processed += 1;
                continue;
            };
            if !matches!(parsed.scheme(), "http" | "https") {
                stats.failed += 1;
                stats.processed += 1;
                continue;
            }
            let domain = domain_key(&parsed);
            if !visited.insert(seed.url.clone()) {
                stats.skipped += 1;
                continue;
            }
            let count = domain_counts.entry(domain).or_default();
            if seed.depth > 0 && *count >= seed.domain_limit.unwrap_or(config.max_per_domain) {
                stats.skipped += 1;
                continue;
            }
            *count += 1;
            stats.processed += 1;
            batch.push(seed);
        }
        if batch.is_empty() {
            continue;
        }
        let fetcher_ref = &fetcher;
        let fetches = stream::iter(batch.into_iter().map(|seed| {
            let delay = if config.kind == CrawlerKind::SearchEngine {
                Duration::ZERO
            } else {
                config.delay
            };
            let fetcher = fetcher_ref;
            let url = seed.url.clone();
            async move {
                let result = fetcher.fetch_page(seed).await;
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                (url, result)
            }
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

        for (requested_url, fetched) in fetches {
            match fetched {
                Ok(mut fetched) if !fetched.noindex => {
                    if fetched.page.is_arabic {
                        stats.arabic += 1;
                    }
                    stats.prices_found += fetched.page.prices.len();
                    if config.kind == CrawlerKind::Advanced {
                        *stats
                            .by_purpose
                            .entry(fetched.seed.purpose.clone())
                            .or_default() += 1;
                    }
                    if config.deduplicate_titles
                        && !fetched.page.title.is_empty()
                        && !title_seen.insert(fetched.page.title.clone())
                    {
                        stats.skipped += 1;
                        continue;
                    }
                    fetched.page.crawled_at = unix_seconds();
                    buffered.push(fetched.page);
                    if buffered.len() >= config.batch_size.max(1) {
                        flush_batch(
                            config.kind,
                            config.conflict_policy,
                            writer,
                            &mut buffered,
                            &mut stats,
                        )
                        .await?;
                    }
                    if fetched.seed.depth < config.max_depth {
                        for link in fetched.discovered_urls {
                            if !visited.contains(&link) {
                                let next = Seed {
                                    url: link,
                                    category: fetched.seed.category.clone(),
                                    purpose: fetched.seed.purpose.clone(),
                                    depth: fetched.seed.depth + 1,
                                    domain_limit: fetched.seed.domain_limit,
                                    extract_prices: fetched.seed.extract_prices,
                                };
                                if config.kind == CrawlerKind::Global {
                                    frontier.push_back(next);
                                } else {
                                    frontier.push_front(next);
                                }
                            }
                        }
                    }
                }
                Ok(_) => stats.skipped += 1,
                Err(error) => {
                    eprintln!(
                        "crawler {:?} failed for {requested_url}: {error}",
                        config.kind
                    );
                    stats.failed += 1;
                }
            }
        }
    }
    flush_batch(
        config.kind,
        config.conflict_policy,
        writer,
        &mut buffered,
        &mut stats,
    )
    .await?;
    Ok(CrawlReport {
        stats,
        domains: domain_counts.len(),
    })
}

/// Run the four source crawler profiles in parallel using their built-in seed
/// catalogs. Startup is staggered by two seconds, matching `run_all_crawlers.py`.
pub async fn run_all_crawlers<W: CloudPageWriter>(
    writer: &W,
) -> Result<Vec<(CrawlerKind, CrawlReport)>, String> {
    let (arabic, global, universities, advanced) = tokio::join!(
        run_profile_after(writer, CrawlerKind::Arabic, Duration::ZERO),
        run_profile_after(writer, CrawlerKind::Global, Duration::from_secs(2)),
        run_profile_after(writer, CrawlerKind::Universities, Duration::from_secs(4)),
        run_profile_after(writer, CrawlerKind::Advanced, Duration::from_secs(6)),
    );
    Ok(vec![
        (CrawlerKind::Arabic, arabic?),
        (CrawlerKind::Global, global?),
        (CrawlerKind::Universities, universities?),
        (CrawlerKind::Advanced, advanced?),
    ])
}

async fn run_profile_after<W: CloudPageWriter>(
    writer: &W,
    kind: CrawlerKind,
    startup_delay: Duration,
) -> Result<CrawlReport, String> {
    if !startup_delay.is_zero() {
        tokio::time::sleep(startup_delay).await;
    }
    let config = match kind {
        CrawlerKind::Arabic => CrawlerConfig::arabic(),
        CrawlerKind::Global => CrawlerConfig::global(),
        CrawlerKind::Universities => CrawlerConfig::universities(),
        CrawlerKind::Advanced => CrawlerConfig::advanced(),
        CrawlerKind::SearchEngine => CrawlerConfig::search_engine(),
    };
    crawl(config, seed_catalog(kind), writer).await
}

/// Concrete Tokio replacement for the Python subprocess monitor. It runs all
/// four profiles concurrently, checks for terminated/failed tasks once a
/// minute, restarts them, reports the latest counts every five minutes, and
/// stops all tasks when `should_stop` becomes true. The caller owns signal or
/// shutdown handling and supplies the cloud writer shared by the tasks.
pub async fn supervise_all_crawlers<W, F>(writer: std::sync::Arc<W>, mut should_stop: F)
where
    W: CloudPageWriter + Send + 'static,
    F: FnMut() -> bool,
{
    use tokio::task::JoinSet;

    let mut tasks: JoinSet<(CrawlerKind, Result<CrawlReport, String>)> = JoinSet::new();
    let staggered = [
        (CrawlerKind::Arabic, Duration::ZERO),
        (CrawlerKind::Global, Duration::from_secs(2)),
        (CrawlerKind::Universities, Duration::from_secs(4)),
        (CrawlerKind::Advanced, Duration::from_secs(6)),
    ];
    let mut active = HashSet::new();
    for (kind, delay) in staggered {
        spawn_profile(&mut tasks, writer.clone(), kind, delay);
        active.insert(kind);
    }

    let mut pending_restart = HashSet::new();
    let mut latest_reports: HashMap<CrawlerKind, CrawlReport> = HashMap::new();
    let mut monitor = tokio::time::interval(Duration::from_secs(60));
    monitor.tick().await;
    let mut last_report = tokio::time::Instant::now();

    loop {
        let mut restart_check = false;
        if tasks.is_empty() {
            monitor.tick().await;
            restart_check = true;
        } else {
            tokio::select! {
                joined = tasks.join_next() => {
                    match joined {
                        Some(Ok((kind, result))) => {
                            active.remove(&kind);
                            match result {
                                Ok(report) => {
                                    eprintln!("crawler {kind:?} stopped: {:?}", report.stats);
                                    latest_reports.insert(kind, report);
                                }
                                Err(error) => {
                                    eprintln!("crawler {kind:?} failed: {error}");
                                }
                            }
                            pending_restart.insert(kind);
                        }
                        Some(Err(error)) => {
                            eprintln!("crawler task panicked: {error}");
                            if let Some(kind) = active.iter().next().copied() {
                                active.remove(&kind);
                                pending_restart.insert(kind);
                            }
                        }
                        None => {}
                    }
                }
                _ = monitor.tick() => {
                    restart_check = true;
                }
            }
        }

        if should_stop() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return;
        }

        if restart_check {
            for kind in pending_restart.drain() {
                spawn_profile(&mut tasks, writer.clone(), kind, Duration::ZERO);
                active.insert(kind);
            }
        }

        if last_report.elapsed() >= Duration::from_secs(300) {
            for kind in [
                CrawlerKind::Arabic,
                CrawlerKind::Global,
                CrawlerKind::Universities,
                CrawlerKind::Advanced,
            ] {
                if let Some(report) = latest_reports.get(&kind) {
                    eprintln!("crawler {kind:?} last run: {:?}", report.stats);
                } else {
                    eprintln!("crawler {kind:?} running");
                }
            }
            last_report = tokio::time::Instant::now();
        }
    }
}

fn spawn_profile<W>(
    tasks: &mut tokio::task::JoinSet<(CrawlerKind, Result<CrawlReport, String>)>,
    writer: std::sync::Arc<W>,
    kind: CrawlerKind,
    startup_delay: Duration,
) where
    W: CloudPageWriter + Send + 'static,
{
    tasks.spawn(async move {
        if !startup_delay.is_zero() {
            tokio::time::sleep(startup_delay).await;
        }
        let config = match kind {
            CrawlerKind::Arabic => CrawlerConfig::arabic(),
            CrawlerKind::Global => CrawlerConfig::global(),
            CrawlerKind::Universities => CrawlerConfig::universities(),
            CrawlerKind::Advanced => CrawlerConfig::advanced(),
            CrawlerKind::SearchEngine => CrawlerConfig::search_engine(),
        };
        let result =
            std::panic::AssertUnwindSafe(crawl(config, seed_catalog(kind), writer.as_ref()))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err("crawler task panicked".into()));
        (kind, result)
    });
}

async fn flush_batch<W: CloudPageWriter>(
    kind: CrawlerKind,
    policy: ConflictPolicy,
    writer: &W,
    batch: &mut Vec<CrawlPage>,
    stats: &mut CrawlStats,
) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }
    let pages = std::mem::take(batch);
    let batch_size = pages.len();
    match writer.write_batch(kind, policy, &pages).await {
        Ok(written) => {
            let written = written.min(batch_size);
            stats.saved += written;
            stats.skipped += batch_size - written;
        }
        Err(error) => {
            eprintln!("cloud batch write failed: {error}");
            stats.failed += batch_size;
        }
    }
    Ok(())
}

async fn read_response_body(
    response: &mut reqwest::Response,
    max_bytes: Option<usize>,
) -> Result<String, FetchError> {
    let mut body = Vec::new();
    let charset = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|content_type| {
            content_type.split(';').find_map(|parameter| {
                let (name, value) = parameter.trim().split_once('=')?;
                (name.trim().eq_ignore_ascii_case("charset"))
                    .then(|| value.trim().trim_matches('"').trim_matches('\'').to_owned())
            })
        });
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| FetchError::Request(error.to_string()))?
    {
        if let Some(limit) = max_bytes {
            if body.len().saturating_add(chunk.len()) > limit {
                return Err(FetchError::BodyTooLarge(limit));
            }
        }
        body.extend_from_slice(&chunk);
    }
    let encoding = charset
        .as_deref()
        .and_then(|label| encoding_rs::Encoding::for_label(label.as_bytes()))
        .unwrap_or(encoding_rs::UTF_8);
    Ok(encoding.decode(&body).0.into_owned())
}

pub fn accepted_status(kind: CrawlerKind, status: u16) -> bool {
    match kind {
        CrawlerKind::Advanced => status == 200 || status == 201,
        CrawlerKind::Global => matches!(status, 200 | 301 | 302),
        _ => status == 200,
    }
}

pub fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429) || (500..600).contains(&status)
}

fn retry_delay(attempt: usize) -> Duration {
    Duration::from_secs(1u64.checked_shl(attempt.min(5) as u32).unwrap_or(32))
}

fn parse_page(
    kind: CrawlerKind,
    source: &str,
    url: &Url,
    seed: &Seed,
    status: u16,
) -> Option<ParsedPage> {
    let cleaned = strip_inert_markup(source, kind);
    let document = Html::parse_document(&cleaned);
    let title_selector = Selector::parse("title").ok()?;
    let meta_selector = Selector::parse("meta").ok()?;
    let html_selector = Selector::parse("html").ok()?;
    let link_selector = Selector::parse("a[href]").ok()?;
    let mut record = CrawlPage {
        url: url.to_string(),
        domain: domain_key(url),
        category: seed.category.clone(),
        purpose: seed.purpose.clone(),
        depth: seed.depth,
        status_code: status,
        ..CrawlPage::default()
    };
    record.crawler = match kind {
        CrawlerKind::Advanced => "UltimateBot/3.0",
        CrawlerKind::Arabic => "FullArabicBot/2.0",
        CrawlerKind::Universities => "UniversitiesBot/2.0",
        CrawlerKind::Global => "GlobalBot/2.0",
        CrawlerKind::SearchEngine => USER_AGENT,
    }
    .to_owned();

    if let Some(title) = document.select(&title_selector).next() {
        record.title = normalize_space(&title.text().collect::<Vec<_>>().join(" "));
    }
    let mut meta = HashMap::new();
    for element in document.select(&meta_selector) {
        let name = element
            .value()
            .attr("name")
            .unwrap_or("")
            .to_ascii_lowercase();
        let property = element
            .value()
            .attr("property")
            .unwrap_or("")
            .to_ascii_lowercase();
        let content = element.value().attr("content").unwrap_or("").trim();
        if content.is_empty() {
            continue;
        }
        if name == "description" || property == "og:description" {
            meta.entry("description").or_insert(content.to_owned());
        }
        if property == "og:description" {
            meta.entry("og_description").or_insert(content.to_owned());
        }
        if name == "keywords" {
            meta.entry("keywords").or_insert(content.to_owned());
        }
        if property == "og:image" {
            meta.entry("og_image").or_insert(content.to_owned());
        }
        if name == "author" {
            meta.entry("author").or_insert(content.to_owned());
        }
        if name == "twitter:description" && !meta.contains_key("description") {
            meta.insert("description", content.to_owned());
        }
        if name == "robots" && content.to_ascii_lowercase().contains("noindex") {
            record.direction = "noindex".into();
        }
    }
    record.description = meta.remove("description").unwrap_or_default();
    record.og_description = meta.remove("og_description").unwrap_or_default();
    record.keywords = meta.remove("keywords").unwrap_or_default();
    record.og_image = meta.remove("og_image").unwrap_or_default();
    record.author = meta.remove("author").unwrap_or_default();

    // Discover the site favicon: a declared <link rel="icon"> tag wins,
    // otherwise fall back to the default {scheme}://{host}/favicon.ico.
    let declared_icon = document
        .select(&Selector::parse("link[rel]").unwrap())
        .filter_map(|element| {
            let rel = element.value().attr("rel").unwrap_or("").to_lowercase();
            if rel.contains("icon") {
                element.value().attr("href")
            } else {
                None
            }
        })
        .next();
    let resolved_icon = declared_icon
        .and_then(|href| url.join(href).ok())
        .filter(|candidate| matches!(candidate.scheme(), "http" | "https"));
    if let Some(icon) = resolved_icon {
        record.favicon = Some(icon.to_string());
    } else if let Some(host) = url.host_str() {
        record.favicon = Some(format!("{}://{}/favicon.ico", url.scheme(), host));
    } else {
        record.favicon = None;
    }

    if let Some(html) = document.select(&html_selector).next() {
        record.html_lang = html.value().attr("lang").unwrap_or("").to_owned();
    }
    if kind == CrawlerKind::SearchEngine {
        let canonical_selector = Selector::parse("link[rel~='canonical'][href]").ok()?;
        if let Some(canonical) = document.select(&canonical_selector).next() {
            if let Some(href) = canonical.value().attr("href") {
                record.canonical = url
                    .join(href)
                    .map(|canonical| canonical.to_string())
                    .unwrap_or_default();
            }
        }
    }

    let content = match kind {
        CrawlerKind::Advanced => {
            let preferred = Selector::parse("main, article, [id*='content' i], [id*='main' i], [class*='content' i], [class*='main' i], [class*='post' i]").ok()?;
            document
                .select(&preferred)
                .next()
                .map(|root| root.text().collect::<Vec<_>>().join(" "))
                .unwrap_or_else(|| document.root_element().text().collect::<Vec<_>>().join(" "))
        }
        _ => document.root_element().text().collect::<Vec<_>>().join(" "),
    };
    let max_chars = match kind {
        CrawlerKind::Advanced => 15_000,
        CrawlerKind::Arabic | CrawlerKind::Universities => 12_000,
        CrawlerKind::Global | CrawlerKind::SearchEngine => usize::MAX,
    };
    record.content = truncate_chars(&normalize_space(&content), max_chars);
    record.word_count = record.content.split_whitespace().count();
    let arabic_count = record
        .title
        .chars()
        .chain(record.content.chars())
        .take(if kind == CrawlerKind::Arabic {
            1000
        } else {
            800
        })
        .filter(|ch| is_arabic(*ch))
        .count();
    record.is_arabic = match kind {
        CrawlerKind::Advanced => arabic_count > 25,
        CrawlerKind::Arabic => arabic_count > 30,
        CrawlerKind::Global => {
            record
                .content
                .chars()
                .take(500)
                .filter(|ch| is_arabic(*ch))
                .count()
                > 20
        }
        _ => false,
    };
    if kind == CrawlerKind::Global {
        record.title = truncate_chars(&record.title, 200);
        if record.title.is_empty() {
            record.title = "No Title".into();
        }
        record.description = truncate_chars(&record.description, 300);
        let (language, direction) = if record
            .content
            .chars()
            .take(500)
            .filter(|ch| is_arabic(*ch))
            .count()
            > 20
        {
            ("ar", "rtl")
        } else {
            ("en", "ltr")
        };
        record.language = language.into();
        record.direction = direction.into();
        record.content = truncate_chars(&record.content, 8_000);
    }
    if kind == CrawlerKind::Advanced {
        record.title = truncate_chars(&record.title, 250);
        record.description = truncate_chars(&record.description, 400);
        record.og_description = truncate_chars(&record.og_description, 400);
        record.keywords = truncate_chars(&record.keywords, 400);
        record.og_image = truncate_chars(&record.og_image, 400);
        record.author = truncate_chars(&record.author, 400);
        if seed
            .extract_prices
            .unwrap_or_else(|| wants_prices(&record.category))
        {
            record.prices = extract_prices(&record.content);
        }
    } else if kind == CrawlerKind::Arabic {
        record.title = truncate_chars(&record.title, 250);
        record.description = truncate_chars(&record.description, 400);
        record.keywords = truncate_chars(&record.keywords, 200);
    } else if kind == CrawlerKind::Universities {
        record.title = truncate_chars(&record.title, 250);
        record.description = truncate_chars(&record.description, 400);
        record.keywords = truncate_chars(&record.keywords, 200);
        record.country = record.category.clone();
    } else if kind == CrawlerKind::SearchEngine {
        record.title = truncate_chars(&record.title, 500);
        record.description = truncate_chars(&record.description, 2_000);
    }

    let skip_extensions = match kind {
        CrawlerKind::Arabic => "pdf|jpg|jpeg|png|gif|mp4|mp3|zip|rar|exe|docx?|xlsx?",
        CrawlerKind::Advanced | CrawlerKind::Universities => {
            "pdf|jpg|jpeg|png|gif|svg|mp4|mp3|zip|rar|exe|docx?|xlsx?|pptx?|css|js"
        }
        CrawlerKind::Global => "pdf|jpg|png|gif|zip|mp4|mp3",
        CrawlerKind::SearchEngine => "",
    };
    let skip_re = if skip_extensions.is_empty() {
        None
    } else {
        Regex::new(&format!(
            "{}\\.({skip_extensions})$",
            if kind == CrawlerKind::Global {
                ""
            } else {
                "(?i)"
            }
        ))
        .ok()
    };
    let limit = match kind {
        CrawlerKind::Advanced => 50,
        CrawlerKind::Arabic | CrawlerKind::Universities => 40,
        CrawlerKind::Global => 8,
        CrawlerKind::SearchEngine => 100,
    };
    let same_domain_only = kind == CrawlerKind::Global || kind == CrawlerKind::SearchEngine;
    let mut links = Vec::new();
    for anchor in document.select(&link_selector) {
        let Some(href) = anchor.value().attr("href") else {
            continue;
        };
        let Ok(target) = url.join(href.trim()) else {
            continue;
        };
        if !matches!(target.scheme(), "http" | "https") {
            continue;
        }
        if skip_re
            .as_ref()
            .is_some_and(|re| re.is_match(target.path()))
        {
            continue;
        }
        if same_domain_only
            && if kind == CrawlerKind::SearchEngine {
                target.host_str() != url.host_str()
            } else {
                domain_key(&target) != domain_key(url)
            }
        {
            continue;
        }
        if kind == CrawlerKind::Advanced
            && matches!(
                record.purpose.as_str(),
                "price_monitoring" | "real_estate" | "job_listings"
            )
            && domain_key(&target) != domain_key(url)
        {
            continue;
        }
        let normalized = normalize_crawl_url_for(kind, &target);
        if !normalized.is_empty() && !links.contains(&normalized) {
            links.push(normalized);
            if links.len() == limit {
                break;
            }
        }
    }
    record.links = links.clone();
    Some(ParsedPage {
        noindex: record.direction == "noindex",
        record,
        links,
    })
}

fn strip_inert_markup(html: &str, kind: CrawlerKind) -> String {
    let tags = match kind {
        CrawlerKind::Global => "script|style|nav|footer|header|aside|noscript|svg|iframe",
        CrawlerKind::SearchEngine => {
            "script|style|nav|footer|header|aside|noscript|svg|iframe"
        }
        CrawlerKind::Arabic | CrawlerKind::Universities => {
            "script|style|nav|footer|header|aside|iframe|noscript"
        }
        CrawlerKind::Advanced => "script|style|nav|footer|header|aside|iframe|noscript|svg",
    };
    tags.split('|').fold(html.to_owned(), |content, tag| {
        let pattern = format!(r"(?is)<{tag}\b[^>]*>.*?</{tag}\s*>");
        match Regex::new(&pattern) {
            Ok(regex) => regex.replace_all(&content, " ").into_owned(),
            Err(error) => {
                eprintln!("Could not strip inert {tag} markup: {error}");
                content
            }
        }
    })
}

fn normalize_space(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn is_arabic(ch: char) -> bool {
    matches!(
        ch as u32,
        0x0600..=0x06ff | 0x0750..=0x077f | 0x08a0..=0x08ff | 0xfb50..=0xfdff | 0xfe70..=0xfeff
    )
}

fn wants_prices(category: &str) -> bool {
    let category = category.to_ascii_lowercase();
    ["price", "real_estate", "job", "سعر", "عقار", "وظائف"]
        .iter()
        .any(|needle| category.contains(needle))
}

fn extract_prices(text: &str) -> Vec<Price> {
    let Ok(regex) = Regex::new(
        r"(?i)(?:([0-9][0-9,]*(?:\.[0-9]+)?)\s*(USD|EUR|GBP|SAR|AED|EGP|ريال|درهم|جنيه)|(?:USD|EUR|GBP|SAR|AED|EGP|ريال|درهم|جنيه)\s*([0-9][0-9,]*(?:\.[0-9]+)?))",
    ) else {
        return Vec::new();
    };
    regex
        .captures_iter(text)
        .take(20)
        .map(|capture| Price {
            value: capture
                .get(1)
                .or_else(|| capture.get(3))
                .map(|m| m.as_str().replace(',', ""))
                .unwrap_or_default(),
            currency: capture
                .get(2)
                .map(|m| m.as_str().to_owned())
                .or_else(|| {
                    capture.get(0).and_then(|m| {
                        [
                            "USD", "EUR", "GBP", "SAR", "AED", "EGP", "ريال", "درهم", "جنيه",
                        ]
                        .iter()
                        .find(|currency| {
                            m.as_str()
                                .to_ascii_lowercase()
                                .contains(&currency.to_ascii_lowercase())
                        })
                        .map(|currency| (*currency).to_owned())
                    })
                })
                .unwrap_or_default(),
        })
        .collect()
}

fn domain_key(url: &Url) -> String {
    url.host_str()
        .map(|host| match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        })
        .unwrap_or_default()
}

/// Legacy advanced/Arabic/university normalization: remove query and
/// fragment, then trim trailing path slashes (including the root slash).
pub fn normalize_crawl_url(url: &Url) -> String {
    normalize_crawl_url_for(CrawlerKind::Advanced, url)
}

fn normalize_crawl_url_for(kind: CrawlerKind, url: &Url) -> String {
    if !matches!(url.scheme(), "http" | "https") {
        return String::new();
    }
    let mut cleaned = url.clone();
    cleaned.set_fragment(None);
    if kind != CrawlerKind::SearchEngine {
        cleaned.set_query(None);
    }
    if matches!(
        kind,
        CrawlerKind::Advanced | CrawlerKind::Arabic | CrawlerKind::Universities
    ) {
        let mut path = cleaned.path().to_owned();
        while path.ends_with('/') {
            path.pop();
        }
        if path.is_empty() {
            path = "/".to_owned();
        }
        cleaned.set_path(&path);
        let mut normalized = cleaned.to_string();
        if normalized.ends_with('/') {
            normalized.pop();
        }
        return normalized;
    }
    cleaned.to_string()
}

fn unix_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Queue integration for `search_engine/crawler.py`. `take_next` reserves one
/// URL; `finish_url` must be called for each reserved URL including robots
/// denials and request/index errors.
pub trait DurableCrawlQueue: Send {
    fn take_next(&mut self) -> QueueFuture<'_, Option<QueuedUrl>>;
    fn canonicalize(&self, url: &str) -> Result<String, String>;
    fn enqueue<'a>(&'a mut self, urls: &'a [String], depth: usize) -> QueueFuture<'a, ()>;
    fn index_document<'a>(&'a mut self, page: &'a CrawlPage) -> QueueFuture<'a, ()>;
    fn finish_url<'a>(&'a mut self, url: &'a str, error: Option<&'a str>) -> QueueFuture<'a, ()>;
    fn stats(&self) -> QueueFuture<'_, Value>;
}

pub type QueueFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, String>> + Send + 'a>>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueuedUrl {
    pub url: String,
    pub depth: usize,
}

pub async fn run_search_queue<Q: DurableCrawlQueue>(
    queue: &mut Q,
    max_pages: usize,
) -> Result<Value, String> {
    let config = CrawlerConfig::search_engine();
    let fetcher = CrawlerFetcher::new(config.clone())?;
    let mut processed = 0usize;
    while processed < max_pages {
        let Some(task) = queue.take_next().await? else {
            break;
        };
        processed += 1;
        let seed = Seed {
            url: task.url.clone(),
            category: String::new(),
            purpose: String::new(),
            depth: task.depth,
            domain_limit: None,
            extract_prices: None,
        };
        let mut fetched = match fetcher.fetch_page(seed).await {
            Ok(fetched) => fetched,
            Err(error) => {
                let message = error.to_string();
                eprintln!("search crawler failed for {}: {message}", task.url);
                queue.finish_url(&task.url, Some(&message)).await?;
                continue;
            }
        };
        if fetched.noindex {
            queue.finish_url(&task.url, None).await?;
            continue;
        }
        let canonical = if fetched.page.canonical.is_empty() {
            fetched.final_url.to_string()
        } else {
            fetched.page.canonical.clone()
        };
        let canonical_url = fetched
            .final_url
            .join(&canonical)
            .unwrap_or_else(|_| fetched.final_url.clone());
        fetched.page.url = queue.canonicalize(canonical_url.as_ref())?;
        fetched.page.crawled_at = unix_seconds();
        queue.index_document(&fetched.page).await?;
        if task.depth < config.max_depth {
            queue
                .enqueue(&fetched.discovered_urls, task.depth + 1)
                .await?;
        }
        queue.finish_url(&task.url, None).await?;
    }
    let mut stats = queue.stats().await?;
    if let Value::Object(ref mut fields) = stats {
        fields.insert("processed".into(), Value::from(processed));
    }
    Ok(stats)
}

/// Celery-compatible route and retry defaults from `celery_config.py`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskPolicy {
    pub queue: &'static str,
    pub max_retries: u32,
    pub retry_delay_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CeleryWorkerConfig {
    pub broker_url: String,
    pub prefetch_multiplier: usize,
    pub max_tasks_per_worker: usize,
    pub hard_time_limit_seconds: u64,
    pub soft_time_limit_seconds: u64,
    pub result_expiry_seconds: u64,
    pub result_backend_max_retries: usize,
    pub broker_heartbeat_seconds: u64,
    pub timezone: &'static str,
}

impl Default for CeleryWorkerConfig {
    fn default() -> Self {
        let broker_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".into());
        Self {
            broker_url,
            prefetch_multiplier: 4,
            max_tasks_per_worker: 1_000,
            hard_time_limit_seconds: 3_600,
            soft_time_limit_seconds: 3_000,
            result_expiry_seconds: 3_600,
            result_backend_max_retries: 10,
            broker_heartbeat_seconds: 10,
            timezone: "UTC",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskTiming {
    pub task_id: String,
    pub task_name: String,
    started_at: tokio::time::Instant,
}

impl TaskTiming {
    pub fn start(task_id: impl Into<String>, task_name: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            task_name: task_name.into(),
            started_at: tokio::time::Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

pub fn task_policy(name: &str) -> TaskPolicy {
    let (queue, delay) = match name {
        "tasks.crawl_url" => ("crawlers", 60),
        "tasks.index_page" => ("indexing", 30),
        "tasks.process_search" => ("search", 0),
        "tasks.generate_embeddings" | "tasks.generate_embeddings_batch" => ("embeddings", 0),
        _ => ("default", 0),
    };
    TaskPolicy {
        queue,
        max_retries: if matches!(name, "tasks.crawl_url" | "tasks.index_page") {
            3
        } else {
            0
        },
        retry_delay_seconds: delay,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduledTask {
    pub name: &'static str,
    pub task: &'static str,
    pub every_seconds: Option<u64>,
    pub cron: Option<&'static str>,
    pub args: Vec<Value>,
}

pub fn celery_schedule() -> Vec<ScheduledTask> {
    vec![
        ScheduledTask {
            name: "run-crawlers-hourly",
            task: "tasks.scheduled_crawl",
            every_seconds: Some(3600),
            cron: None,
            args: vec![Value::from("hourly")],
        },
        ScheduledTask {
            name: "update-pagerank-daily",
            task: "tasks.update_pagerank",
            every_seconds: None,
            cron: Some("0 2 * * *"),
            args: vec![],
        },
        ScheduledTask {
            name: "sync-to-cloud",
            task: "tasks.sync_cloud_data",
            every_seconds: Some(1800),
            cron: None,
            args: vec![],
        },
        ScheduledTask {
            name: "clean-old-logs",
            task: "tasks.cleanup_old_data",
            every_seconds: None,
            cron: Some("0 3 * * 0"),
            args: vec![],
        },
        ScheduledTask {
            name: "generate-embeddings-nightly",
            task: "tasks.generate_embeddings_batch",
            every_seconds: None,
            cron: Some("0 1 * * *"),
            args: vec![],
        },
        // These two schedules are registered dynamically in `tasks.py`.
        ScheduledTask {
            name: "daily-cleanup",
            task: "tasks.cleanup_old_data",
            every_seconds: None,
            cron: Some("0 3 * * *"),
            args: vec![],
        },
        ScheduledTask {
            name: "hourly-cloud-sync",
            task: "tasks.sync_to_cloud",
            every_seconds: Some(3600),
            cron: None,
            args: vec![],
        },
    ]
}

pub trait TaskQueue: Send {
    fn enqueue(&mut self, task: TaskEnvelope) -> QueueFuture<'_, String>;
    fn retry<'a>(
        &'a mut self,
        task_id: &'a str,
        countdown_seconds: u64,
        reason: &'a str,
    ) -> QueueFuture<'a, ()>;
    fn complete<'a>(&'a mut self, task_id: &'a str, result: Value) -> QueueFuture<'a, ()>;
    fn fail<'a>(&'a mut self, task_id: &'a str, error: &'a str) -> QueueFuture<'a, ()>;
}

pub type TaskFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Cloud/vector backend for the operations in `tasks.py`. Implementations
/// should make `index_page`, search, embeddings, sync, and cleanup operate on
/// the existing cloud stores; the old SQLite-based indexing/search/sync and
/// `VACUUM` paths are intentionally not recreated.
pub trait TaskBackend: Send {
    fn crawl_url(&mut self, url: String, depth: usize) -> TaskFuture<'_>;
    fn index_page(&mut self, url: String, title: String, content: String) -> TaskFuture<'_>;
    fn process_search(&mut self, query: String, limit: usize) -> TaskFuture<'_>;
    fn generate_embeddings_batch(&mut self, page_ids: Vec<i64>) -> TaskFuture<'_>;
    fn sync_to_cloud(&mut self, batch_size: usize) -> TaskFuture<'_>;
    fn cleanup_old_data(&mut self, days: usize) -> TaskFuture<'_>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskEnvelope {
    pub name: String,
    pub args: Vec<Value>,
    pub retries: u32,
}

/// Execute a supported Celery task while preserving its route/retry policy.
/// `crawl_urls_batch` retains group semantics by scheduling one `crawl_url`
/// task per URL; a host may use its native group primitive instead.
pub async fn execute_task<B: TaskBackend, Q: TaskQueue>(
    task_id: &str,
    task: &TaskEnvelope,
    backend: &mut B,
    queue: &mut Q,
) -> Result<(), String> {
    let result = match task.name.as_str() {
        "tasks.crawl_url" => {
            let url = string_arg(task, 0)?;
            let depth = usize_arg(task, 1, 1)?;
            backend.crawl_url(url, depth).await
        }
        "tasks.crawl_urls_batch" => {
            let urls: Vec<String> = serde_json::from_value(
                task.args
                    .first()
                    .cloned()
                    .ok_or("crawl_urls_batch requires URL list")?,
            )
            .map_err(|error| error.to_string())?;
            let depth = usize_arg(task, 1, 1)?;
            let mut ids = Vec::with_capacity(urls.len());
            for url in urls {
                ids.push(
                    queue
                        .enqueue(TaskEnvelope {
                            name: "tasks.crawl_url".into(),
                            args: vec![Value::String(url), Value::from(depth)],
                            retries: 0,
                        })
                        .await?,
                );
            }
            Ok(serde_json::json!({ "task_ids": ids }))
        }
        "tasks.index_page" => {
            backend
                .index_page(
                    string_arg(task, 0)?,
                    string_arg(task, 1)?,
                    string_arg(task, 2)?,
                )
                .await
        }
        "tasks.process_search" => {
            let query = string_arg(task, 0)?;
            backend.process_search(query, usize_arg(task, 1, 20)?).await
        }
        "tasks.generate_embeddings_batch" => {
            let ids: Vec<i64> = serde_json::from_value(
                task.args
                    .first()
                    .cloned()
                    .ok_or("embedding batch requires page IDs")?,
            )
            .map_err(|error| error.to_string())?;
            backend.generate_embeddings_batch(ids).await
        }
        "tasks.sync_to_cloud" => backend.sync_to_cloud(usize_arg(task, 0, 100)?).await,
        "tasks.cleanup_old_data" => backend.cleanup_old_data(usize_arg(task, 0, 30)?).await,
        unsupported => return Err(format!("unsupported legacy task: {unsupported}")),
    };

    match result {
        Ok(value) => queue.complete(task_id, value).await,
        Err(error) => {
            let policy = task_policy(&task.name);
            if task.retries < policy.max_retries {
                queue
                    .retry(task_id, policy.retry_delay_seconds, &error)
                    .await
            } else {
                queue.fail(task_id, &error).await
            }
        }
    }
}

fn string_arg(task: &TaskEnvelope, index: usize) -> Result<String, String> {
    task.args
        .get(index)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("{} requires string argument {index}", task.name))
}

fn usize_arg(task: &TaskEnvelope, index: usize, default: usize) -> Result<usize, String> {
    match task.args.get(index) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                format!(
                    "{} requires nonnegative integer argument {index}",
                    task.name
                )
            }),
    }
}

pub async fn enqueue_crawl_batch<Q: TaskQueue>(
    queue: &mut Q,
    urls: &[String],
    depth: usize,
) -> Result<Vec<String>, String> {
    let mut task_ids = Vec::with_capacity(urls.len());
    for url in urls {
        task_ids.push(
            queue
                .enqueue(TaskEnvelope {
                    name: "tasks.crawl_url".into(),
                    args: vec![Value::String(url.clone()), Value::from(depth)],
                    retries: 0,
                })
                .await?,
        );
    }
    Ok(task_ids)
}

/// `/upload` payload matching the prior API shape. The source label is
/// caller-supplied; the legacy `"sqlite-local"` default is not generated.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UploadDocument {
    pub url: String,
    pub title: String,
    pub content: String,
    pub source: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UploadResult {
    pub uploaded: usize,
    pub errors: usize,
}

#[derive(Clone, Debug)]
pub struct BulkUploadClient {
    pub api_base: String,
    pub batch_size: usize,
    pub timeout: Duration,
}

impl Default for BulkUploadClient {
    fn default() -> Self {
        Self {
            api_base: std::env::var("VELO_API_BASE")
                .unwrap_or_else(|_| "http://127.0.0.1:8000".into()),
            batch_size: 10,
            timeout: Duration::from_secs(30),
        }
    }
}

impl BulkUploadClient {
    pub async fn test_api(&self) -> bool {
        let Ok(client) = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        else {
            return false;
        };
        matches!(client.get(&self.api_base).send().await, Ok(response) if response.status().as_u16() == 200)
    }

    pub async fn upload_batch(&self, batch: &[UploadDocument]) -> UploadResult {
        let Ok(client) = reqwest::Client::builder().timeout(self.timeout).build() else {
            return UploadResult {
                uploaded: 0,
                errors: batch.len(),
            };
        };
        match client
            .post(format!("{}/upload", self.api_base.trim_end_matches('/')))
            .json(batch)
            .send()
            .await
        {
            Ok(response) if response.status().as_u16() == 200 => {
                response.json().await.unwrap_or(UploadResult {
                    uploaded: 0,
                    errors: batch.len(),
                })
            }
            _ => UploadResult {
                uploaded: 0,
                errors: batch.len(),
            },
        }
    }
}

/// The legacy source was a local SQLite `pages` table. Supply export batches
/// from the existing cloud store/API instead; there is deliberately no
/// SQLite-backed implementation.
pub trait CloudExportSource {
    fn next_batch(&mut self, max_items: usize) -> Result<Option<Vec<UploadDocument>>, String>;
}

pub async fn bulk_upload_from_cloud<S: CloudExportSource>(
    client: &BulkUploadClient,
    source: &mut S,
) -> Result<UploadResult, String> {
    let mut total = UploadResult::default();
    while let Some(batch) = source.next_batch(client.batch_size.max(1))? {
        if batch.is_empty() {
            break;
        }
        let result = client.upload_batch(&batch).await;
        total.uploaded += result.uploaded;
        total.errors += result.errors;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(total)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrawlerProcessSpec {
    pub name: &'static str,
    pub rust_entrypoint: &'static str,
    pub description: &'static str,
}

pub const RUN_ALL_CRAWLERS: [CrawlerProcessSpec; 4] = [
    CrawlerProcessSpec {
        name: "Arabic-Crawler",
        rust_entrypoint: "CrawlerKind::Arabic",
        description: "200+ Arabic sites",
    },
    CrawlerProcessSpec {
        name: "Global-Crawler",
        rust_entrypoint: "CrawlerKind::Global",
        description: "200+ global sites",
    },
    CrawlerProcessSpec {
        name: "Universities-Crawler",
        rust_entrypoint: "CrawlerKind::Universities",
        description: "1000+ universities",
    },
    CrawlerProcessSpec {
        name: "Ultimate-Crawler",
        rust_entrypoint: "CrawlerKind::Advanced",
        description: "varied content",
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessStatus {
    pub id: u64,
    pub running: bool,
    pub exit_code: Option<i32>,
}

/// Process/supervision boundary replacing Python subprocess orchestration.
pub trait CrawlerSupervisor {
    fn start(&mut self, spec: CrawlerProcessSpec) -> Result<u64, String>;
    fn status(&mut self, process_id: u64) -> Result<ProcessStatus, String>;
    fn terminate(&mut self, process_id: u64) -> Result<(), String>;
}

#[derive(Clone, Debug)]
pub struct SupervisedCrawler {
    pub spec: CrawlerProcessSpec,
    pub process_id: u64,
    pub started_at: tokio::time::Instant,
}

pub fn start_all_crawlers<S: CrawlerSupervisor>(
    supervisor: &mut S,
) -> Result<Vec<SupervisedCrawler>, String> {
    let mut running = Vec::with_capacity(RUN_ALL_CRAWLERS.len());
    for spec in RUN_ALL_CRAWLERS {
        let process_id = supervisor.start(spec)?;
        running.push(SupervisedCrawler {
            spec,
            process_id,
            started_at: tokio::time::Instant::now(),
        });
    }
    Ok(running)
}

pub fn restart_stopped_crawlers<S: CrawlerSupervisor>(
    supervisor: &mut S,
    running: &mut [SupervisedCrawler],
) -> Result<(), String> {
    for entry in running {
        if !supervisor.status(entry.process_id)?.running {
            entry.process_id = supervisor.start(entry.spec)?;
            entry.started_at = tokio::time::Instant::now();
        }
    }
    Ok(())
}

pub fn stop_all_crawlers<S: CrawlerSupervisor>(
    supervisor: &mut S,
    running: &[SupervisedCrawler],
) -> Result<(), String> {
    for entry in running {
        supervisor.terminate(entry.process_id)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    fn serve_responses(responses: Vec<(u16, String, Vec<u8>)>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            for (status, content_type, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 512];
                loop {
                    let read = stream.read(&mut chunk).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let reason = match status {
                    200 => "OK",
                    404 => "Not Found",
                    503 => "Service Unavailable",
                    _ => "Test",
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(&body).unwrap();
                stream.flush().unwrap();
            }
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn crawler_profiles_preserve_limits_and_write_policies() {
        let advanced = CrawlerConfig::advanced();
        assert_eq!(
            (
                advanced.max_depth,
                advanced.concurrency,
                advanced.max_pages,
                advanced.batch_size
            ),
            (3, 20, 500_000, 100)
        );
        assert_eq!(advanced.conflict_policy, ConflictPolicy::TidbInsertIgnore);

        let arabic = CrawlerConfig::arabic();
        assert_eq!(
            (
                arabic.max_depth,
                arabic.concurrency,
                arabic.max_pages,
                arabic.max_per_domain
            ),
            (4, 15, 100_000, 200)
        );
        assert_eq!(arabic.conflict_policy, ConflictPolicy::CockroachDoNothing);

        let universities = CrawlerConfig::universities();
        assert_eq!(
            (
                universities.max_depth,
                universities.concurrency,
                universities.max_pages,
                universities.max_per_domain
            ),
            (3, 15, 200_000, 150)
        );

        let global = CrawlerConfig::global();
        assert_eq!(
            (
                global.max_depth,
                global.max_pages,
                global.max_per_domain,
                global.batch_size
            ),
            (2, 500, 5, 10)
        );
        let search = CrawlerConfig::search_engine();
        assert!(search.respect_robots);
        assert_eq!(search.max_response_bytes, Some(2_000_000));
    }

    #[test]
    fn built_in_seed_catalogs_match_source_counts_and_metadata() {
        let advanced = seed_catalog(CrawlerKind::Advanced);
        assert_eq!(advanced.len(), 185);
        assert_eq!(
            advanced
                .iter()
                .filter(|seed| seed.category == "مراقبة_أسعار")
                .count(),
            15
        );
        assert!(advanced
            .iter()
            .filter(|seed| seed.category == "مراقبة_أسعار")
            .all(|seed| {
                seed.purpose == "price_monitoring"
                    && seed.domain_limit == Some(500)
                    && seed.extract_prices == Some(true)
            }));
        assert_eq!(seed_catalog(CrawlerKind::Arabic).len(), 149);
        assert_eq!(seed_catalog(CrawlerKind::Universities).len(), 325);
        assert_eq!(seed_catalog(CrawlerKind::Global).len(), 154);
        assert!(seed_catalog(CrawlerKind::SearchEngine).is_empty());
    }

    #[test]
    fn url_cleanup_matches_crawler_link_rules() {
        let url = Url::parse("https://Example.org/path/?q=one#part").unwrap();
        assert_eq!(normalize_crawl_url(&url), "https://example.org/path");
        let root = Url::parse("https://example.org/").unwrap();
        assert_eq!(normalize_crawl_url(&root), "https://example.org");
        assert_eq!(
            normalize_crawl_url_for(CrawlerKind::Global, &root),
            "https://example.org/"
        );
        assert_eq!(
            normalize_crawl_url_for(CrawlerKind::SearchEngine, &url),
            "https://example.org/path/?q=one"
        );
    }

    #[test]
    fn robots_allow_precedence_and_user_agent_groups_are_applied() {
        let rules = RobotsRules::parse(
            "User-agent: *\nDisallow: /private\nAllow: /private/public\nUser-agent: VeloSearchBot\nDisallow: /bot-only\n",
            USER_AGENT,
        );
        assert!(!rules.allows(&Url::parse("https://example.org/bot-only").unwrap()));
        assert!(rules.allows(&Url::parse("https://example.org/private/public/page").unwrap()));
        assert!(rules.allows(&Url::parse("https://example.org/private/other").unwrap()));
    }

    #[test]
    fn search_page_parsing_honors_noindex_canonical_and_link_normalization() {
        let url = Url::parse("https://example.org/a/index.html").unwrap();
        let seed = Seed::new(url.as_str(), "test");
        let page = parse_page(
            CrawlerKind::SearchEngine,
            r#"<html lang="ar"><head><title>عنوان</title>
               <meta name="robots" content="noindex,follow">
               <link rel="canonical" href="/canonical?tracking=1">
               <meta name="description" content="وصف"></head>
               <body><script>hidden secret</script><main>نص فهرسة
               <a href="../next/?x=1#fragment">next</a>
               <a href="https://elsewhere.test/">external</a></main></body></html>"#,
            &url,
            &seed,
            200,
        )
        .unwrap();
        assert!(page.noindex);
        assert_eq!(
            page.record.canonical,
            "https://example.org/canonical?tracking=1"
        );
        assert!(!page.record.content.contains("hidden secret"));
        assert_eq!(page.links, vec!["https://example.org/next/?x=1"]);
    }

    #[test]
    fn task_routes_retry_policies_and_schedule_are_preserved() {
        assert_eq!(
            task_policy("tasks.crawl_url"),
            TaskPolicy {
                queue: "crawlers",
                max_retries: 3,
                retry_delay_seconds: 60
            }
        );
        assert_eq!(task_policy("tasks.index_page").retry_delay_seconds, 30);
        assert_eq!(task_policy("tasks.generate_embeddings").queue, "embeddings");
        assert_eq!(celery_schedule().len(), 7);
        let worker = CeleryWorkerConfig::default();
        assert_eq!(worker.prefetch_multiplier, 4);
        assert_eq!(worker.hard_time_limit_seconds, 3_600);
        assert_eq!(worker.timezone, "UTC");
    }

    #[test]
    fn all_crawler_runner_uses_four_rust_profiles() {
        assert_eq!(RUN_ALL_CRAWLERS.len(), 4);
        assert_eq!(
            RUN_ALL_CRAWLERS.map(|spec| spec.rust_entrypoint),
            [
                "CrawlerKind::Arabic",
                "CrawlerKind::Global",
                "CrawlerKind::Universities",
                "CrawlerKind::Advanced"
            ]
        );
    }

    #[test]
    fn process_supervision_starts_restarts_and_stops_each_profile() {
        #[derive(Default)]
        struct FakeSupervisor {
            next_id: u64,
            statuses: HashMap<u64, ProcessStatus>,
            starts: Vec<CrawlerProcessSpec>,
        }

        impl CrawlerSupervisor for FakeSupervisor {
            fn start(&mut self, spec: CrawlerProcessSpec) -> Result<u64, String> {
                self.next_id += 1;
                let id = self.next_id;
                self.starts.push(spec);
                self.statuses.insert(
                    id,
                    ProcessStatus {
                        id,
                        running: true,
                        exit_code: None,
                    },
                );
                Ok(id)
            }

            fn status(&mut self, process_id: u64) -> Result<ProcessStatus, String> {
                self.statuses
                    .get(&process_id)
                    .cloned()
                    .ok_or_else(|| format!("unknown process {process_id}"))
            }

            fn terminate(&mut self, process_id: u64) -> Result<(), String> {
                let status = self
                    .statuses
                    .get_mut(&process_id)
                    .ok_or_else(|| format!("unknown process {process_id}"))?;
                status.running = false;
                status.exit_code = Some(0);
                Ok(())
            }
        }

        let mut supervisor = FakeSupervisor::default();
        let mut running = start_all_crawlers(&mut supervisor).unwrap();
        assert_eq!(supervisor.starts, RUN_ALL_CRAWLERS);
        assert_eq!(running.len(), RUN_ALL_CRAWLERS.len());

        let stopped_id = running[1].process_id;
        supervisor.statuses.get_mut(&stopped_id).unwrap().running = false;
        restart_stopped_crawlers(&mut supervisor, &mut running).unwrap();
        assert_ne!(running[1].process_id, stopped_id);
        assert_eq!(supervisor.starts[4], RUN_ALL_CRAWLERS[1]);

        stop_all_crawlers(&mut supervisor, &running).unwrap();
        assert!(running.iter().all(|crawler| {
            !supervisor
                .statuses
                .get(&crawler.process_id)
                .unwrap()
                .running
        }));
    }

    #[test]
    fn supported_domains_accept_subdomains_but_reject_lookalikes() {
        let fetcher = CrawlerFetcher::new(CrawlerConfig::advanced())
            .unwrap()
            .with_supported_domains(["example.test".to_owned()]);
        assert!(fetcher.is_supported_url(&Url::parse("https://www.example.test/page").unwrap()));
        assert!(!fetcher.is_supported_url(&Url::parse("https://notexample.test/page").unwrap()));
        assert!(!fetcher
            .is_supported_url(&Url::parse("https://example.test.evil.invalid/page").unwrap()));
    }

    #[tokio::test]
    async fn fetch_page_decodes_charset_and_extracts_page_fields_and_links() {
        let body = b"<html><head><title>caf\xe9</title><meta name=\"description\" content=\"caf\xe9 info\"></head><body><main>caf\xe9 content<a href=\"/next/?q=1#section\">next</a></main></body></html>".to_vec();
        let (base, server) =
            serve_responses(vec![(200, "text/html; charset=windows-1252".into(), body)]);
        let fetcher = CrawlerFetcher::new(CrawlerConfig::advanced().with_body_limit(1024)).unwrap();
        let fetched = fetcher
            .fetch_page(Seed::new(format!("{base}/start"), "test"))
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(fetched.page.title, "café");
        assert_eq!(fetched.page.description, "café info");
        assert!(fetched.page.content.contains("café content"));
        assert_eq!(fetched.discovered_urls, vec![format!("{base}/next")]);
        assert_eq!(fetched.status_code, 200);
    }

    #[tokio::test]
    async fn fetch_page_retries_transient_status_and_enforces_body_limit() {
        let (base, server) = serve_responses(vec![
            (503, "text/plain".into(), b"retry".to_vec()),
            (
                200,
                "text/html; charset=utf-8".into(),
                b"<html><title>recovered</title><body>ok</body></html>".to_vec(),
            ),
        ]);
        let fetcher = CrawlerFetcher::new(
            CrawlerConfig::advanced()
                .with_retry_limit(1)
                .with_body_limit(1024),
        )
        .unwrap();
        let fetched = fetcher
            .fetch_page(Seed::new(format!("{base}/retry"), "test"))
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(fetched.page.title, "recovered");

        let (base, server) = serve_responses(vec![(
            200,
            "text/html; charset=utf-8".into(),
            b"<html><title>too large</title><body>the body exceeds the configured limit</body></html>"
                .to_vec(),
        )]);
        let fetcher = CrawlerFetcher::new(CrawlerConfig::advanced().with_body_limit(16)).unwrap();
        let error = fetcher
            .fetch_page(Seed::new(format!("{base}/large"), "test"))
            .await
            .unwrap_err();
        server.join().unwrap();
        assert_eq!(error, FetchError::BodyTooLarge(16));
    }

    #[test]
    fn fetch_status_and_retry_classification_is_explicit() {
        assert!(accepted_status(CrawlerKind::Advanced, 201));
        assert!(!accepted_status(CrawlerKind::Arabic, 201));
        assert!(accepted_status(CrawlerKind::Global, 302));
        assert!(!accepted_status(CrawlerKind::SearchEngine, 302));
        assert!(retryable_status(429));
        assert!(retryable_status(503));
        assert!(!retryable_status(404));
    }

    #[test]
    fn bulk_upload_defaults_keep_batch_size_without_local_database() {
        let client = BulkUploadClient::default();
        assert_eq!(client.batch_size, 10);
        assert_eq!(client.api_base, "http://127.0.0.1:8000");
    }

    #[test]
    fn favicon_extracts_declared_icon() {
        let url = Url::parse("https://example.com/page").unwrap();
        let seed = Seed::new(url.to_string(), "test");
        let html = r#"<html><head><link rel="icon" href="/favicon.ico"></head><body>hi</body></html>"#;
        let parsed = parse_page(CrawlerKind::SearchEngine, html, &url, &seed, 200).unwrap();
        assert_eq!(parsed.record.favicon.as_deref(), Some("https://example.com/favicon.ico"));
    }

    #[test]
    fn favicon_resolves_relative_href() {
        let url = Url::parse("https://example.com/deep/page.html").unwrap();
        let seed = Seed::new(url.to_string(), "test");
        let html = r#"<html><head><link rel="shortcut icon" href="img/site.ico"></head></html>"#;
        let parsed = parse_page(CrawlerKind::SearchEngine, html, &url, &seed, 200).unwrap();
        assert_eq!(
            parsed.record.favicon.as_deref(),
            Some("https://example.com/deep/img/site.ico")
        );
    }

    #[test]
    fn favicon_falls_back_to_default_when_undeclared() {
        let url = Url::parse("https://example.com/").unwrap();
        let seed = Seed::new(url.to_string(), "test");
        let html = "<html><head><title>no icon</title></head><body>hi</body></html>";
        let parsed = parse_page(CrawlerKind::Arabic, html, &url, &seed, 200).unwrap();
        assert_eq!(parsed.record.favicon.as_deref(), Some("https://example.com/favicon.ico"));
    }

    #[test]
    fn favicon_ignores_non_http_href() {
        let url = Url::parse("https://example.com/").unwrap();
        let seed = Seed::new(url.to_string(), "test");
        let html = r#"<html><head><link rel="icon" href="ftp://cdn.example.com/x.ico"></head></html>"#;
        let parsed = parse_page(CrawlerKind::SearchEngine, html, &url, &seed, 200).unwrap();
        assert_eq!(parsed.record.favicon.as_deref(), Some("https://example.com/favicon.ico"));
    }
}
