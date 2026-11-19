//! Keyboard-layout recovery for Arabic and English queries.
//!
//! Searchers frequently leave the wrong layout active, so an intended Arabic
//! word arrives as the Latin keys that produce it ("lwv" for "مصر") or an
//! intended English word arrives as the Arabic letters of those keys ("اثممخ"
//! for "hello"). Both directions are mapped through the standard Arabic (101)
//! layout, and callers only keep a conversion that lands on a known word.

/// Latin key → Arabic letter(s) on the standard Arabic (101) layout.
const LATIN_TO_ARABIC: [(char, &str); 34] = [
    ('`', "ذ"),
    ('q', "ض"),
    ('w', "ص"),
    ('e', "ث"),
    ('r', "ق"),
    ('t', "ف"),
    ('y', "غ"),
    ('u', "ع"),
    ('i', "ه"),
    ('o', "خ"),
    ('p', "ح"),
    ('[', "ج"),
    (']', "د"),
    ('a', "ش"),
    ('s', "س"),
    ('d', "ي"),
    ('f', "ب"),
    ('g', "ل"),
    ('h', "ا"),
    ('j', "ت"),
    ('k', "ن"),
    ('l', "م"),
    (';', "ك"),
    ('\'', "ط"),
    ('z', "ئ"),
    ('x', "ء"),
    ('c', "ؤ"),
    ('v', "ر"),
    ('b', "لا"),
    ('n', "ى"),
    ('m', "ة"),
    (',', "و"),
    ('.', "ز"),
    ('/', "ظ"),
];

/// Arabic letter → Latin key, the exact inverse of `LATIN_TO_ARABIC`.
const ARABIC_TO_LATIN: [(char, char); 33] = [
    ('ذ', '`'),
    ('ض', 'q'),
    ('ص', 'w'),
    ('ث', 'e'),
    ('ق', 'r'),
    ('ف', 't'),
    ('غ', 'y'),
    ('ع', 'u'),
    ('ه', 'i'),
    ('خ', 'o'),
    ('ح', 'p'),
    ('ج', '['),
    ('د', ']'),
    ('ش', 'a'),
    ('س', 's'),
    ('ي', 'd'),
    ('ب', 'f'),
    ('ل', 'g'),
    ('ا', 'h'),
    ('ت', 'j'),
    ('ن', 'k'),
    ('م', 'l'),
    ('ك', ';'),
    ('ط', '\''),
    ('ئ', 'z'),
    ('ء', 'x'),
    ('ؤ', 'c'),
    ('ر', 'v'),
    ('ى', 'n'),
    ('ة', 'm'),
    ('و', ','),
    ('ز', '.'),
    ('ظ', '/'),
];

/// Converts Latin keys to the Arabic letters they produce. Returns `None`
/// when the input has no convertible letter or already contains Arabic.
pub fn latin_to_arabic(input: &str) -> Option<String> {
    convert_latin_to_arabic(input).filter(|converted| converted != input)
}

fn convert_latin_to_arabic(input: &str) -> Option<String> {
    if input.chars().any(is_arabic_letter) {
        return None;
    }
    let mut converted = String::with_capacity(input.len());
    let mut changed = false;
    for ch in input.chars() {
        match LATIN_TO_ARABIC.iter().find(|(key, _)| *key == ch) {
            Some((_, arabic)) => {
                converted.push_str(arabic);
                changed = true;
            }
            None => converted.push(ch),
        }
    }
    changed.then_some(converted)
}

/// Converts Arabic letters to the Latin keys that produce them, undoing a
/// query typed while the English layout was active. Returns `None` when the
/// input contains no Arabic letter.
pub fn arabic_to_latin(input: &str) -> Option<String> {
    convert_arabic_to_latin(input).filter(|converted| converted != input)
}

fn convert_arabic_to_latin(input: &str) -> Option<String> {
    if !input.chars().any(is_arabic_letter) {
        return None;
    }
    let characters: Vec<char> = input.chars().collect();
    let mut converted = String::with_capacity(characters.len());
    let mut index = 0;
    while index < characters.len() {
        // "لا" is the single Latin `b` key on the Arabic layout.
        if characters[index] == 'ل'
            && characters.get(index + 1) == Some(&'ا')
        {
            converted.push('b');
            index += 2;
            continue;
        }
        match ARABIC_TO_LATIN
            .iter()
            .find(|(arabic, _)| *arabic == characters[index])
        {
            Some((_, latin)) => converted.push(*latin),
            None => converted.push(characters[index]),
        }
        index += 1;
    }
    Some(converted)
}

fn is_arabic_letter(ch: char) -> bool {
    ('\u{0620}'..='\u{064A}').contains(&ch) || ('\u{066E}'..='\u{06D3}').contains(&ch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latin_keys_become_the_intended_arabic_word() {
        assert_eq!(latin_to_arabic("lwv").as_deref(), Some("مصر"));
        assert_eq!(latin_to_arabic("l]vsm").as_deref(), Some("مدرسة"));
    }

    #[test]
    fn arabic_letters_become_the_intended_latin_word() {
        assert_eq!(arabic_to_latin("اثممخ").as_deref(), Some("hello"));
        assert_eq!(arabic_to_latin("قعسف").as_deref(), Some("rust"));
    }

    #[test]
    fn already_correct_text_is_left_alone() {
        assert_eq!(latin_to_arabic("مصر"), None);
        assert_eq!(arabic_to_latin("hello"), None);
        assert_eq!(latin_to_arabic(""), None);
        assert_eq!(arabic_to_latin(""), None);
    }
}
