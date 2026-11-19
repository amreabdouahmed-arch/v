use std::collections::HashMap;

pub struct ArabicLemmatizer;
pub struct EnglishLemmatizer;
pub struct Lemmatizer;

fn arabic_char(c: char) -> bool {
    ('\u{0600}'..='\u{06ff}').contains(&c)
}

impl ArabicLemmatizer {
    pub fn lemmatize(word: &str) -> String {
        let word = word.trim();
        let dictionary = [
            ("الحاسوب", "حاسوب"),
            ("البرمجة", "برمجة"),
            ("البرمجيات", "برمجة"),
            ("المبرمج", "برمجة"),
            ("الكمبيوتر", "حاسوب"),
            ("الحاسب", "حاسوب"),
            ("المواقع", "موقع"),
            ("الموقع", "موقع"),
            ("التطبيقات", "تطبيق"),
            ("التطبيق", "تطبيق"),
            ("الجامعات", "جامعة"),
            ("الجامعة", "جامعة"),
            ("الطلاب", "طالب"),
            ("الطالب", "طالب"),
            ("الدراسة", "دراسة"),
            ("الدراسات", "دراسة"),
            ("التقنيات", "تقنية"),
            ("التقنية", "تقنية"),
            ("التكنولوجيا", "تقنية"),
            ("اللغات", "لغة"),
            ("اللغة", "لغة"),
            ("البرامج", "برنامج"),
            ("البرنامج", "برنامج"),
            ("الأنظمة", "نظام"),
            // The hamza spelling normalizes to the bare-alef form, so both
            // keys must resolve (queries arrive pre-normalized).
            ("الانظمة", "نظام"),
            ("النظام", "نظام"),
            ("البيانات", "بيانات"),
            ("المعلومات", "معلومات"),
            ("الشبكات", "شبكة"),
            ("الشبكة", "شبكة"),
            ("الخوادم", "خادم"),
            ("الخادم", "خادم"),
            ("السيرفرات", "سيرفر"),
            ("السيرفر", "سيرفر"),
            ("المستخدمين", "مستخدم"),
            ("المستخدم", "مستخدم"),
            ("العملاء", "عميل"),
            ("العميل", "عميل"),
            ("العملية", "عمل"),
            ("العمليات", "عمل"),
            ("العمل", "عمل"),
            ("الوظيفة", "وظيفة"),
            ("الوظائف", "وظيفة"),
            ("المهام", "مهمة"),
            ("المهمة", "مهمة"),
        ];
        if let Some((_, lemma)) = dictionary.iter().find(|(key, _)| *key == word) {
            return (*lemma).to_owned();
        }
        // Only genuine prefixed forms are stripped: one of the common Arabic
        // clitic prefixes followed by a stem of at least three characters.
        // The former 4-character heuristics mangled real words (طالب→طلب,
        // تعلم→علم, حاسب→حسب) and created false synonym keys.
        for prefix in ["ال", "وال", "فال", "بال", "كال", "لل"] {
            if let Some(stem) = word.strip_prefix(prefix) {
                if stem.chars().count() >= 3 {
                    return stem.to_owned();
                }
            }
        }
        word.to_owned()
    }

    pub fn lemmatize_text(text: &str) -> Vec<String> {
        text.split_whitespace().map(Self::lemmatize).collect()
    }
}

impl EnglishLemmatizer {
    fn irregular() -> HashMap<&'static str, &'static str> {
        [
            ("running", "run"),
            ("ran", "run"),
            ("going", "go"),
            ("went", "go"),
            ("gone", "go"),
            ("making", "make"),
            ("made", "make"),
            ("taking", "take"),
            ("took", "take"),
            ("taken", "take"),
            ("coming", "come"),
            ("came", "come"),
            ("giving", "give"),
            ("gave", "give"),
            ("given", "give"),
            ("knowing", "know"),
            ("knew", "know"),
            ("known", "know"),
            ("thinking", "think"),
            ("thought", "think"),
            ("bringing", "bring"),
            ("brought", "bring"),
            ("buying", "buy"),
            ("bought", "buy"),
            ("teaching", "teach"),
            ("taught", "teach"),
            ("catching", "catch"),
            ("caught", "catch"),
            ("building", "build"),
            ("built", "build"),
            ("sending", "send"),
            ("sent", "send"),
            ("spending", "spend"),
            ("spent", "spend"),
            ("sitting", "sit"),
            ("sat", "sit"),
            ("standing", "stand"),
            ("stood", "stand"),
            ("understanding", "understand"),
            ("understood", "understand"),
            ("winning", "win"),
            ("won", "win"),
            ("losing", "lose"),
            ("lost", "lose"),
            ("finding", "find"),
            ("found", "find"),
            ("flying", "fly"),
            ("flew", "fly"),
            ("flown", "fly"),
            ("driving", "drive"),
            ("drove", "drive"),
            ("driven", "drive"),
            ("rising", "rise"),
            ("rose", "rise"),
            ("risen", "rise"),
            ("shining", "shine"),
            ("shone", "shine"),
            ("writing", "write"),
            ("wrote", "write"),
            ("written", "write"),
            ("riding", "ride"),
            ("rode", "ride"),
            ("ridden", "ride"),
            ("speaking", "speak"),
            ("spoke", "speak"),
            ("spoken", "speak"),
            ("breaking", "break"),
            ("broke", "break"),
            ("broken", "break"),
            ("choosing", "choose"),
            ("chose", "choose"),
            ("chosen", "choose"),
            ("freezing", "freeze"),
            ("froze", "freeze"),
            ("frozen", "freeze"),
            ("stealing", "steal"),
            ("stole", "steal"),
            ("stolen", "steal"),
            ("waking", "wake"),
            ("woke", "wake"),
            ("woken", "wake"),
            ("getting", "get"),
            ("got", "get"),
            ("gotten", "get"),
            ("beginning", "begin"),
            ("began", "begin"),
            ("begun", "begin"),
            ("drinking", "drink"),
            ("drank", "drink"),
            ("drunk", "drink"),
            ("singing", "sing"),
            ("sang", "sing"),
            ("sung", "sing"),
            ("swimming", "swim"),
            ("swam", "swim"),
            ("swum", "swim"),
            ("ringing", "ring"),
            ("rang", "ring"),
            ("rung", "ring"),
            ("children", "child"),
            ("people", "person"),
            ("men", "man"),
            ("women", "woman"),
            ("teeth", "tooth"),
            ("feet", "foot"),
            ("mice", "mouse"),
            ("geese", "goose"),
            ("oxen", "ox"),
            ("leaves", "leaf"),
            ("loaves", "loaf"),
            ("lives", "life"),
            ("wives", "wife"),
            ("knives", "knife"),
            ("shelves", "shelf"),
            ("thieves", "thief"),
            ("wolves", "wolf"),
            ("calves", "calf"),
            ("halves", "half"),
            ("scarves", "scarf"),
        ]
        .into_iter()
        .collect()
    }

    pub fn lemmatize(word: &str) -> String {
        let word = word.trim().to_lowercase();
        if word.is_empty() {
            return word;
        }
        if let Some(lemma) = Self::irregular().get(word.as_str()) {
            return (*lemma).to_owned();
        }
        for (suffix, replacement) in [
            ("ies", "y"),
            ("ied", "y"),
            ("ying", "ie"),
            ("ying", "y"),
            ("ing", ""),
            ("ly", ""),
            ("ed", ""),
            ("ied", "y"),
            ("ies", "y"),
            ("es", ""),
            ("s", ""),
        ] {
            if word.ends_with(suffix) && word.chars().count() > suffix.chars().count() + 2 {
                let mut lemma = word[..word.len() - suffix.len()].to_owned();
                lemma.push_str(replacement);
                return lemma;
            }
        }
        word
    }

    pub fn lemmatize_text(text: &str) -> Vec<String> {
        let mut words = Vec::new();
        let mut word = String::new();
        for c in text.chars() {
            if c.is_ascii_alphabetic() {
                word.push(c.to_ascii_lowercase());
            } else if !word.is_empty() {
                words.push(Self::lemmatize(&word));
                word.clear();
            }
        }
        if !word.is_empty() {
            words.push(Self::lemmatize(&word));
        }
        words
    }
}

impl Lemmatizer {
    pub fn lemmatize(word: &str) -> String {
        if word.chars().any(arabic_char) {
            ArabicLemmatizer::lemmatize(word)
        } else {
            EnglishLemmatizer::lemmatize(word)
        }
    }
    pub fn lemmatize_list(words: &[String]) -> Vec<String> {
        words.iter().map(|word| Self::lemmatize(word)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_and_irregular_english_forms() {
        assert_eq!(Lemmatizer::lemmatize("الجامعات"), "جامعة");
        assert_eq!(Lemmatizer::lemmatize("children"), "child");
        assert_eq!(Lemmatizer::lemmatize("running"), "run");
    }

    #[test]
    fn text_tokenization_matches_language_specific_methods() {
        assert_eq!(
            EnglishLemmatizer::lemmatize_text("Studies, walked!"),
            ["study", "walk"]
        );
        assert_eq!(
            ArabicLemmatizer::lemmatize_text("البرمجة الحاسوب"),
            ["برمجة", "حاسوب"]
        );
    }

    #[test]
    fn four_char_heuristics_no_longer_mangle_words() {
        // Real words that the old 4-character rules destroyed.
        assert_eq!(Lemmatizer::lemmatize("طالب"), "طالب");
        assert_eq!(Lemmatizer::lemmatize("تعلم"), "تعلم");
        assert_eq!(Lemmatizer::lemmatize("حاسب"), "حاسب");
        assert_eq!(Lemmatizer::lemmatize("دارس"), "دارس");
        // Genuine prefixed forms are still stripped.
        assert_eq!(Lemmatizer::lemmatize("والوظيفة"), "وظيفة");
        // Both spellings of "الأنظمة" reach the dictionary lemma.
        assert_eq!(Lemmatizer::lemmatize("الانظمة"), "نظام");
    }
}
