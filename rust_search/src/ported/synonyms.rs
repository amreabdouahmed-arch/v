//! A small curated Arabic synonym map, expanded locally — no external service.
//!
//! Sparse queries are rewritten into an OR-expanded Tantivy query, e.g.
//! "امتحان الجامعة" also matches "اختبار" or "كلية" pages. Lookups normalize
//! and lemmatize query tokens first, so inflected forms find their stem's
//! group. The map is a few dozen curated groups; it costs nothing to load.

use std::collections::HashMap;

use crate::ported::common::normalize_text;
use crate::ported::lemmatizer::Lemmatizer;

/// Curated synonym groups. Entries are plain stem forms in normalized
/// spelling (bare alef, no diacritics). Group order is preserved in the
/// expanded query so suggestions stay deterministic.
const SYNONYM_GROUPS: &[&[&str]] = &[
    &["جامعة", "جامعات", "كلية", "كليات", "معهد", "معاهد", "اكاديمية"],
    &["طالب", "طلاب", "دارس", "متعلم"],
    &["امتحان", "امتحانات", "اختبار", "اختبارات", "فحص"],
    &["دراسة", "دراسات", "بحث", "ابحاث", "مرجع"],
    &["شرح", "درس", "دروس", "مقدمة"],
    &["تعلم", "تدريب", "تدريبات", "دورة", "دورات", "كورس", "كورسات"],
    &["وظيفة", "وظائف", "عمل", "شغل"],
    &["سعر", "اسعار", "ثمن", "تكلفة"],
    &["برنامج", "برامج", "تطبيق", "تطبيقات"],
    &["تحميل", "تنزيل"],
    &["حاسوب", "حاسب", "كمبيوتر", "لابتوب"],
    &["تقنية", "تكنولوجيا"],
    &["موقع", "مواقع", "صفحة", "صفحات"],
    &["معلومة", "معلومات", "بيانات"],
    &["مجاني", "مجانية", "مجان", "مجانا"],
    &["منحة", "منح"],
    &["نتيجة", "نتائج"],
    &["قبول", "تسجيل"],
    &["university", "college", "institute"],
    &["exam", "test", "quiz"],
];

/// Egyptian dialect synonym groups - maps colloquial Egyptian Arabic to formal Arabic
const EGYPTIAN_DIALECT_GROUPS: &[&[&str]] = &[
    &["عايز", "اريد", "احتاج", "محتاج", "حاجة"],
    &["عايزة", "عايز", "اريد", "احتاج", "محتاج", "حاجة"],
    &["شغل", "عمل", "وظيفة", "مهنة", "حرفة", "business"],
    &["البيت", "المنزل", "الشقة", "house"],
    &["المكتبة", "الكتاب", "كتاب", "book"],
    &["المحيط", "سماعات", "هاتف", "headphone"],
    &["الموبايل", "الهاتف", "هاتف", "mobile", "phone"],
    &["الكمبيوتر", "الحاسوب", "لابتوب", "computer", "PC"],
    &["الانترنت", "ويب", "شبكة", "internet"],
    &["البرمجة", "رموز", "كود", "programming", "code"],
    &["التصميم", "رسومات", "design"],
    &["الصورة", "شكل", "photo", "picture"],
    &["الساعة", "الوقت", "وقت", "time"],
    &["الأكل", "الطعام", "مأكولات", "food"],
    &["الشرب", "الماء", "مشروبات", "drink"],
    &["كده", "كده", "كده", "like"],
    &["إيه", "ايه", "ماذا", "ما", "what"],
    &["فور", "الان", "حالا", "سريع", "now"],
    &["الفلوس", "المال", "نقود", "money", "cash"],
    &["المحل", "دكان", "متجر", "store", "shop"],
    &["الشارع", "الطريق", "street"],
    &["المدينة", "قرية", "بلد", "city"],
    &["الطيارة", "طائرة", "plane", "airplane"],
    &["السفينة", "مركب", "ship", "boat"],
    &["العربية", "سيارة", "car", "auto"],
    &["الوقت", "ساعة", "دقيقة", "ثانية", "time"],
    &["الصيف", "شتاء", "ربيع", "خريف", "season"],
    &["الحر", "برد", "مطر", "ثلج", "weather"],
    &["الأكل", "طعام", "مأكولات", "food"],
    &["الشرب", "ماء", "مشروبات", "drink"],
    &["الصحة", "مريض", "عيادة", "health", "medical"],
    &["صوت", "موسيقى", "أغنية", "sound", "music"],
    &["مصري", "مصر", "Egyptian", "Egypt"],
    &["عربي", "عربية", "Arabic"],
    &["أفريقي", "افريقيا", "Africa"],
    &["إنجليزي", "انجليزي", "English"],
    &["فرنسي", "فرنسا", "French"],
    &["ألماني", "المانيا", "German"],
    &["إسباني", "اسبانيا", "Spanish"],
    &["تركي", "تركيا", "Turkish"],
    &["هندي", "الهند", "Indian"],
    &["صيني", "الصين", "Chinese"],
    &["ياباني", "اليابان", "Japanese"],
    &["كوري", "كوريا", "Korean"],
    &["روسي", "روسيا", "Russian"],
    &["إيطالي", "ايطاليا", "Italian"],
    &["يوناني", "اليونان", "Greek"],
    &["هولندي", "هولندا", "Dutch"],
    &["سويدي", "السويد", "Swedish"],
    &["نرويجي", "النرويج", "Norwegian"],
    &["دنماركي", "الدنمارك", "Danish"],
    &["فنلندي", "فنلندا", "Finnish"],
    &["بولندي", "بولندا", "Polish"],
    &["تشيكي", "التشيك", "Czech"],
    &["مجري", "المجر", "Hungarian"],
    &["روماني", "رومانيا", "Romanian"],
    &["يوناني", "اليونان", "Greek"],
    &["تركي", "تركيا", "Turkish"],
    &["عربي", "العربية", "Arabic"],
    &["فارسي", "فارس", "Persian"],
    &["أردي", "باكستان", "Urdu"],
    &["بنغالي", "بنغلاديش", "Bengali"],
    &["هندي", "الهند", "Hindi"],
    &["تايلاندي", "تايلاند", "Thai"],
    &["فيتنامي", "فيتنام", "Vietnamese"],
    &["إندونيسي", "اندونيسيا", "Indonesian"],
    &["فلبيني", "الفلبين", "Filipino"],
    &["ماليزي", "ماليزيا", "Malaysian"],
    &["سنغافوري", "سنغافورة", "Singaporean"],
    &["أسترالي", "استراليا", "Australian"],
    &["نيوزيلندي", "نيوزيلندا", "New Zealand"],
    &["كندي", "كندا", "Canadian"],
    &["أمريكي", "امريكا", "American"],
    &["برازيلي", "البرازيل", "Brazilian"],
    &["مكسيكي", "المكسيك", "Mexican"],
    &["argentinian", "argentina", "Argentinian"],
    &["chilean", "chile", "Chilean"],
    &["colombian", "colombia", "Colombian"],
    &["peruvian", "peru", "Peruvian"],
    &["venezuelan", "venezuela", "Venezuelan"],
    &["ecuadorian", "ecuador", "Ecuadorian"],
    &["bolivian", "bolivia", "Bolivian"],
    &["paraguayan", "paraguay", "Paraguayan"],
    &["uruguayan", "uruguay", "Uruguayan"],
    &["cuban", "cuba", "Cuban"],
    &["dominican", "dominican", "Dominican"],
    &["puerto", "puerto", "Puerto Rican"],
    &["jamaican", "jamaica", "Jamaican"],
    &["haitian", "haiti", "Haitian"],
    &["trinidadian", "trinidad", "Trinidadian"],
    &["barbadian", "barbados", "Barbadian"],
    &["bahamian", "bahamas", "Bahamian"],
    &["grenadian", "grenada", "Grenadian"],
    &["vincentian", "saint vincent", "Vincentian"],
    &["lucian", "saint lucia", "Lucian"],
    &["antiguan", "antigua", "Antiguan"],
    &["barbudan", "barbuda", "Barbudan"],
    &["kittsian", "saint kitts", "Kittsian"],
    &["nevisian", "nevis", "Nevisian"],
    &["montserratian", "montserrat", "Montserratian"],
    &["anguillan", "anguilla", "Anguillan"],
    &["bvi", "bvi", "British Virgin Islander"],
    &["caymanian", "cayman", "Caymanian"],
    &["bermudian", "bermuda", "Bermudian"],
    &["turks", "turks", "Turks and Caicos"],
    &["caicos", "caicos", "Caicos Islander"],
    &["bahamian", "bahamas", "Bahamian"],
    &["cuban", "cuba", "Cuban"],
    &["dominican", "dominican", "Dominican"],
    &["puerto", "puerto", "Puerto Rican"],
    &["jamaican", "jamaica", "Jamaican"],
    &["haitian", "haiti", "Haitian"],
    &["trinidadian", "trinidad", "Trinidadian"],
    &["barbadian", "barbados", "Barbadian"],
    &["bahamian", "bahamas", "Bahamian"],
    &["grenadian", "grenada", "Grenadian"],
    &["vincentian", "saint vincent", "Vincentian"],
    &["lucian", "saint lucia", "Lucian"],
    &["antiguan", "antigua", "Antiguan"],
    &["barbudan", "barbuda", "Barbudan"],
    &["kittsian", "saint kitts", "Kittsian"],
    &["nevisian", "nevis", "Nevisian"],
    &["montserratian", "montserrat", "Montserratian"],
    &["anguillan", "anguilla", "Anguillan"],
    &["bvi", "bvi", "British Virgin Islander"],
    &["caymanian", "cayman", "Caymanian"],
    &["bermudian", "bermuda", "Bermudian"],
    &["turks", "turks", "Turks and Caicos"],
    &["caicos", "caicos", "Caicos Islander"],
];

pub struct Synonyms {
    index: HashMap<String, Vec<String>>,
}

impl Default for Synonyms {
    fn default() -> Self {
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for group in SYNONYM_GROUPS {
            let words: Vec<String> = group.iter().map(|word| normalize_text(word)).collect();
            for word in &words {
                let lemma = Lemmatizer::lemmatize(word);
                for key in [word.as_str(), lemma.as_str()] {
                    let members = index.entry(key.to_owned()).or_default();
                    for member in &words {
                        if !members.contains(member) {
                            members.push(member.clone());
                        }
                    }
                }
            }
        }
        Self { index }
    }
}

impl Synonyms {
    /// Same-group words for one token, excluding the token itself.
    pub fn expansions(&self, token: &str) -> Vec<String> {
        let normalized = normalize_text(token);
        if normalized.is_empty() {
            return Vec::new();
        }
        let lemma = Lemmatizer::lemmatize(&normalized);
        let mut words = self.index.get(&normalized).cloned().unwrap_or_default();
        if lemma != normalized {
            for member in self.index.get(&lemma).into_iter().flatten() {
                if !words.contains(member) {
                    words.push(member.clone());
                }
            }
        }
        words.retain(|word| *word != normalized);
        words
    }

    /// Rewrites a plain-text query into an OR-expanded Tantivy query when at
    /// least one token has synonyms; queries that already use Tantivy syntax
    /// are returned untouched as `None`.
    pub fn expand_query(&self, query: &str) -> Option<String> {
        if query.contains(['"', ':', '(', ')', '+', '-', '^', '*'])
            // Uppercase boolean keywords: normalization would lowercase them
            // into literal search terms and destroy the user's intent.
            || query
                .split_whitespace()
                .any(|token| matches!(token, "AND" | "OR" | "NOT"))
        {
            return None;
        }
        let normalized = normalize_text(query);
        let mut expanded = Vec::new();
        let mut changed = false;
        for token in normalized.split_whitespace() {
            let synonyms = self.expansions(token);
            if synonyms.is_empty() {
                expanded.push(token.to_owned());
            } else {
                let mut group = vec![token.to_owned()];
                group.extend(synonyms);
                expanded.push(format!("({})", group.join(" OR ")));
                changed = true;
            }
        }
        changed.then(|| expanded.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inflected_forms_find_their_synonym_group() {
        let synonyms = Synonyms::default();
        let expansions = synonyms.expansions("الجامعة");
        assert!(expansions.contains(&"كلية".to_owned()));
        assert!(expansions.contains(&"معهد".to_owned()));
        assert!(expansions.contains(&"اكاديمية".to_owned()));
        assert!(synonyms.expansions("الطلاب").contains(&"طالب".to_owned()));
        assert!(synonyms
            .expansions("امتحانات")
            .contains(&"اختبار".to_owned()));
    }

    #[test]
    fn unknown_tokens_expand_to_nothing() {
        let synonyms = Synonyms::default();
        assert!(synonyms.expansions("فلوربي").is_empty());
        assert!(synonyms.expansions("").is_empty());
    }

    #[test]
    fn plain_queries_expand_into_or_groups() {
        let synonyms = Synonyms::default();
        let expanded = synonyms.expand_query("امتحان الجامعة").unwrap();
        assert!(expanded.contains("(امتحان OR امتحانات OR اختبار"));
        assert!(expanded.contains("اختبارات"));
        assert!(expanded.contains("كلية"));
        assert!(expanded.contains("معهد"));
    }

    #[test]
    fn tantivy_syntax_is_left_untouched() {
        let synonyms = Synonyms::default();
        assert!(synonyms.expand_query(r#"title:"الجامعة""#).is_none());
        assert!(synonyms.expand_query("rust -python").is_none());
    }

    #[test]
    fn queries_without_synonyms_return_none() {
        let synonyms = Synonyms::default();
        assert!(synonyms.expand_query("فلوربي زوربي").is_none());
    }

    #[test]
    fn boolean_keywords_stop_expansion() {
        let synonyms = Synonyms::default();
        assert!(synonyms.expand_query("rust AND python").is_none());
        assert!(synonyms.expand_query("جامعة OR كلية").is_none());
        assert!(synonyms.expand_query("جامعة NOT كلية").is_none());
    }

    #[test]
    fn mangled_lemma_no_longer_merges_groups() {
        let synonyms = Synonyms::default();
        // "درس" must only pull the شرح group, not the طالب group the old
        // lemmatizer merged into it through the false "درس" key.
        let lesson = synonyms.expansions("درس");
        assert!(lesson.contains(&"شرح".to_owned()));
        assert!(!lesson.contains(&"طالب".to_owned()));
        // "طلب" must not expand into the student group at all.
        assert!(synonyms.expansions("طلب").is_empty());
        // "علم" (science) must not pull the training group.
        assert!(!synonyms.expansions("علم").contains(&"تدريب".to_owned()));
    }
}
