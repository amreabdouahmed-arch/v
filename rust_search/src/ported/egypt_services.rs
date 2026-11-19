//! Egypt Services Instant Box - provides quick info for common Egyptian government services

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgyptServiceInfo {
    pub service_name: String,
    pub summary: String,
    pub documents_needed: Vec<String>,
    pub official_url: String,
    pub steps: Vec<String>,
    pub contact: Option<String>,
}

pub struct EgyptServicesBox;

impl EgyptServicesBox {
    pub fn get_service_info(&self, query: &str) -> Option<EgyptServiceInfo> {
        let query_lower = query.to_lowercase();
        
        if query_lower.contains("بطاقة رقم قومي") || query_lower.contains("national id") || query_lower.contains("card") {
            Some(EgyptServiceInfo {
                service_name: "استخراج بطاقة رقم قومية جديدة".to_string(),
                summary: "بطاقة رقم قومية للمواطنين المصريين فوق 15 سنة".to_string(),
                documents_needed: vec![
                    "صورة من شهادة الميلاد".to_string(),
                    "صورة من شهادة النجاح (للمميزين)".to_string(),
                    "صور شخصية حديثة (4 صور)".to_string(),
                    "صورة من بطاقة الخدمة الوطنية (للمستهدفين)".to_string(),
                ],
                official_url: "https://www.emis.gov.eg".to_string(),
                steps: vec![
                    "زيارة أقرب مكتب EMIS".to_string(),
                    "تقديم المستندات المطلوبة".to_string(),
                    "دفع الرسوم (ج.م 150-300 حسب النوع)".to_string(),
                    "استلام البطاقة خلال 30 يوم".to_string(),
                ],
                contact: Some("+202 1234567".to_string()),
            })
        } else if query_lower.contains("التنسيق") || query_lower.contains("تنسيق") || query_lower.contains("registration") {
            Some(EgyptServiceInfo {
                service_name: "التسجيل في الجامعات المصرية".to_string(),
                summary: "التسجيل للجامعات المصرية من خلال البوابة الموحدة".to_string(),
                documents_needed: vec![
                    "شهادة الثانوية العامة".to_string(),
                    "بطاقة رقم قومي".to_string(),
                    "صور شخصية".to_string(),
                    "شهادة أهلية (لبعض المناهج)".to_string(),
                ],
                official_url: "https://www.emis.gov.eg/Universities".to_string(),
                steps: vec![
                    "إنشاء حساب على بوابة الجامعة".to_string(),
                    "تحميل المستندات المطلوبة".to_string(),
                    "اختيار الكليات المفضلة".to_string(),
                    "دفع رسوم التسجيل".to_string(),
                ],
                contact: Some("+202 1234568".to_string()),
            })
        } else if query_lower.contains("قطار") || query_lower.contains("train") {
            Some(EgyptServiceInfo {
                service_name: "حجز تذاكر القطارات المصرية".to_string(),
                summary: "حجز تذاكر القطارات داخل مصر من خلال الموقع الرسمي أو التطبيق".to_string(),
                documents_needed: vec![
                    "بطاقة رقم قومي".to_string(),
                    "رقم الهاتف".to_string(),
                ],
                official_url: "https://www.enr.gov.eg".to_string(),
                steps: vec![
                    "إنشاء حساب على موقع ENR".to_string(),
                    "اختيار نقطة الانطلاق والوجهة".to_string(),
                    "اختيار تاريخ ووقت السفر".to_string(),
                    "الدفع عبر بطاقة الائتمان أو فوري".to_string(),
                ],
                contact: Some("+202 1234569".to_string()),
            })
        } else if query_lower.contains("سعر الدولار") || query_lower.contains("dollar") || query_lower.contains("البنك") {
            Some(EgyptServiceInfo {
                service_name: "أسعار الدولار في مصر".to_string(),
                summary: "أسعار الصرف الحالية للدولار الأمريكي واليورو والجنيه الإسترليني في البنوك المصرية".to_string(),
                documents_needed: vec![],
                official_url: "https://www.cbe.org.eg".to_string(),
                steps: vec![
                    "زيارة موقع البنك المركزي المصري".to_string(),
                    "التحقق من أسعار الصرف اليومية".to_string(),
                    "مقارنة الأسعار عبر مختلف البنوك".to_string(),
                ],
                contact: Some("+202 1234570".to_string()),
            })
        } else if query_lower.contains("ضريبة") || query_lower.contains("tax") {
            Some(EgyptServiceInfo {
                service_name: "تقديم إقرار ضريبي".to_string(),
                summary: "تقديم الإقرارات الضريبية للشركات والأفراد في مصر".to_string(),
                documents_needed: vec![
                    "بطاقة رقم قومي".to_string(),
                    "سندات دفع ضريبية".to_string(),
                    "مستندات الإيرادات".to_string(),
                ],
                official_url: "https://www.eta.gov.eg".to_string(),
                steps: vec![
                    "إنشاء حساب على بوابة مصلحة الضرائب المصرية".to_string(),
                    "تحميل المستندات الضريبية".to_string(),
                    "حساب المسؤولية الضريبية".to_string(),
                    "الدفع عبر فوري أو التحويل البنكي".to_string(),
                ],
                contact: Some("+202 1234571".to_string()),
            })
        } else if query_lower.contains("جواز") || query_lower.contains("passport") {
            Some(EgyptServiceInfo {
                service_name: "استخراج أو تجديد جواز سفر مصري".to_string(),
                summary: "التقديم على جواز سفر مصري جديد أو تجديد جواز سفر مصري".to_string(),
                documents_needed: vec![
                    "بطاقة رقم قومي".to_string(),
                    "صورة من شهادة الميلاد".to_string(),
                    "صور شخصية حديثة (6 صور)".to_string(),
                    "إثبات السكن".to_string(),
                ],
                official_url: "https://www.mfa.gov.eg".to_string(),
                steps: vec![
                    "زيارة أقرب مكتب جوازات".to_string(),
                    "تقديم المستندات المطلوبة".to_string(),
                    "دفع الرسوم (ج.م 300-500)".to_string(),
                    "استلام جواز السفر خلال 15 يوم".to_string(),
                ],
                contact: Some("+202 1234572".to_string()),
            })
        } else if query_lower.contains("امتحان") || query_lower.contains("exam") {
            Some(EgyptServiceInfo {
                service_name: "التسجيل في الامتحانات".to_string(),
                summary: "التسجيل للامتحانات المعيارية في مصر".to_string(),
                documents_needed: vec![
                    "بطاقة رقم قومي".to_string(),
                    "صور شخصية".to_string(),
                ],
                official_url: "https://www.emis.gov.eg/Exams".to_string(),
                steps: vec![
                    "إنشاء حساب على بوابة الامتحانات".to_string(),
                    "اختيار نوع الامتحان".to_string(),
                    "دفع رسوم التسجيل".to_string(),
                ],
                contact: Some("+202 1234573".to_string()),
            })
        } else if query_lower.contains("توصيل") || query_lower.contains("delivery") {
            Some(EgyptServiceInfo {
                service_name: "توصيل الطرود والشاحنات".to_string(),
                summary: "تتبع وإدارة توصيل الطرود والشاحنات".to_string(),
                documents_needed: vec![
                    "رقم الطلب".to_string(),
                    "بطاقة رقم قومي".to_string(),
                ],
                official_url: "https://www.post.gov.eg".to_string(),
                steps: vec![
                    "زيارة موقع مصر للبريد".to_string(),
                    "إدخال رقم التتبع الخاص بك".to_string(),
                    "تتبع طردك".to_string(),
                ],
                contact: Some("+202 1234574".to_string()),
            })
        } else if query_lower.contains("فواتير") || query_lower.contains("bills") {
            Some(EgyptServiceInfo {
                service_name: "سداد الفواتير الرئيسية".to_string(),
                summary: "سداد فواتير الخدمات الرئيسية في مصر".to_string(),
                documents_needed: vec![
                    "رقم العميل".to_string(),
                    "بطاقة رقم قومي".to_string(),
                ],
                official_url: "https://www.fawry.com".to_string(),
                steps: vec![
                    "زيارة موقع فوري أو التطبيق".to_string(),
                    "إدخال رمز المفوتر ورقم العميل".to_string(),
                    "الدفع عبر بطاقة الائتمان أو النقد".to_string(),
                ],
                contact: Some("+202 1234575".to_string()),
            })
        } else {
            None
        }
    }
}