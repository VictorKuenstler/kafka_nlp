import faust


class TermFrequency(faust.Record):
    document: str
    term: str
    tf: float
    inverse_document: int
    document_count: int


class TFIDF_Score(faust.Record):
    document: str
    tfidf: float
    term: str
