import faust


class TermFrequency(faust.Record):
    document: str
    term: str
    tf: float
    document_count: int


class TFIDF_Score(faust.Record):
    document: str
    tfidf: float
    term: str
