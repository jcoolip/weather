import re


salary_pattern = re.compile(
    r'(?P<currency>[$€£])?\s*(?P<min>\d[\d,\.]*k?)\s*(?:[-–]\s*(?P<max>\d[\d,\.]*k?))?\s*(?:/?\s*(?P<freq>hour|hr|year|yr|month|mo|day)?)',
    re.IGNORECASE
)

def parse_salary(text):
    m = salary_pattern.search(text)
    if not m:
        return None

    currency = m.group("currency")
    smin = m.group("min")
    smax = m.group("max")
    freq = m.group("freq")

    def normalize(v):
        if not v:
            return None
        v = v.replace(",", "").lower()
        if "k" in v:
            return float(v.replace("k", "")) * 1000
        return float(v)

    return normalize(smin), normalize(smax), freq, currency

test = [
    "$90k/year",
    "$45/hour",
    "120k - 200k",
    "€60k–€80k annually",
    "$120,000",
    "$55/hr",
    "£70k per year",
    "$100k-$140k OTE",
    "$120k base",
    "$80k–$100k / year",
    "$50,000 - $70,000 / yr",
    "$30/hr - $50/hr",
    "USD 90k - 110k per annum",
    "$100k",
    "$60k–$75k / year",
    "45k-55k / yr",
    "$200/day",
    "$80/hr + bonus",
    "$90,000–$120,000",
    "120k–150k per year",
]
for x in test:
    print(parse_salary(x))