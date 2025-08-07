import json

STOPWORDS = set([
    'the', 'corporation', 'healthcare', 'Pharmaceuticals', 'pharma', 'resources', 'inc', 'us', 'sa', 'spa', 'ab',
    'gmbh',
    'ag', 'asp', 'co', 'ltd', 'llc', 'bv', 'corp', 'formerly', 'previously', 'by', 'as', 'dba',
    'com', 'laboratories', 'systems', 'global', 'sales', 'products',
    'media group', 'group', 'limited', 'solutions', 'software', 'computing', 'networks',
    'brands', 'project', 'services', 'technologies', 'technology', 'companies', 'holdings', 'studios', 'digital',
    'mobile', 'financial', 'management', 'media', 'maintenance', 'agency', 'support', 'bank', 'reseller', 'account',
    'internal', 'external', 'international', 'university', 'labs', 'enterprises', 'industries',
    'incorporated', 'incorporation', 'entertainment', 'society', 'security', 'interactive', 'capital'
])


def preprocess(name: str) -> str:
    if not isinstance(name, str):
        name = ""
    name = name.lower()
    name = name.replace("corp.", "corporation")
    name = ''.join([char for char in name if char.isalpha() or char.isspace() or char == '-'])
    name = ' '.join([word for word in name.split() if word not in STOPWORDS])
    return name


def convert_txt_to_set(input_file):
    keywords_set = set()
    with open(input_file, 'r') as f:
        for line in f:
            cleaned_line = preprocess(line.strip())
            if cleaned_line:
                keywords_set.add(cleaned_line)

    with open('/\\dojscrape\\converted_keywords.json',
              'w') as f:
        json.dump(list(keywords_set), f)


convert_txt_to_set('/\\dojscrape\\op_suppliers.txt')
