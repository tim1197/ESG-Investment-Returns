import hashlib
import re
from bs4 import BeautifulSoup
from pypdf import PdfReader


SPECIAL_CHARACTERS = ["\xa0", "☒", "☐", "_"]
SENTENCE_PATTERN = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
PDF_CACHE = "cache/report.pdf"


class TextExtractor:
    def __init__(self, report):
        self.report = report

    def remove_special_characters(self, text):
        for char in SPECIAL_CHARACTERS:
            char = re.escape(char)
            text = re.sub(char, "", text)
        return text

    def reduce_multiple_spaces(self, text):
        return re.sub(r"\s+", " ", text)

    def extract_sentences(self, text):
        return re.split(SENTENCE_PATTERN, text)

    def clean(self, sentence):
        cleaned_sentences = sentence.strip()
        cleaned_sentences = self.remove_special_characters(cleaned_sentences)
        cleaned_sentences = self.reduce_multiple_spaces(cleaned_sentences)
        return cleaned_sentences

    def extract_text_from_pdf(self):
        reader = PdfReader(self.report)
        texts = []
        for page in reader.pages:
            texts.append(page.extract_text())
        return "".join(texts)

    def store_pdf_cache(self):
        with open(PDF_CACHE, "wb") as f:
            f.write(self.report.content)

    def create_hash(self, texts: list):
        raw_text = "".join(texts)
        return hashlib.sha256(raw_text.encode()).hexdigest()

    def get_sentences(self, url_type="htm"):
        if url_type == "pdf":
            self.store_pdf_cache()
            texts = self.extract_text_from_pdf()
        else:
            texts = self.report.text
        soup = BeautifulSoup(texts, "html.parser")
        text = soup.get_text()
        sentences = self.extract_sentences(text)
        return [self.clean(sentence) for sentence in sentences if sentence.strip()]

    def get_scentences_dax(self):
        reader = PdfReader(self.report)
        texts = []
        for page in reader.pages:
            texts.append(page.extract_text())
        text = "".join(texts)
        sentences = self.extract_sentences(text)
        return [self.clean(sentence) for sentence in sentences if sentence.strip()]
