## Badges

[![License](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](http://creativecommons.org/licenses/by/4.0/) [![Python 3.1](https://img.shields.io/badge/python-3.1-blue.svg)](https://www.python.org/downloads/release/python-300/) [![PyPI - Downloads](https://img.shields.io/pypi/dd/crawley-lite)]()

# Crawley-Lite: A tool for basic web crawling

Crawley-Lite is a command-line tool engineered in Python, that facilitates the process of web platform discovery. It enables users to customize search queries and validate search results, with the overarching goal of identifying websites that are underpinned by specific technological platforms (i.e. Semantic Wikis, Open Data Portals, Wordpress sites). 

## Features

- Cross-Platform Search Engine Crawling: The tool is equipped with the ability to automate the crawling of popular search engines (only Google implemented). 
- Rule-Based HTML Parsing: Crawley-Lite can parse HTML content derived from search results, facilitating identification and confirmation of underlying technical platforms via the application of custom validation rules.
- Hyperlink Traversal and Validation: The tool is capable of hyperlink traversal, following and validating underlying platforms for further URLs extracted from the initial query search results.
- Result Categorization: The output from Crawley-Lite is classified acc. to the technology and applicable markers. 

## Requirements

- Python 3.x
- urllib
- bs4
- serpapi

## Installation

Clone this repository to your machine and install the required packages.

## Pre-requisites

To be able to use the tool, you need SERP API key(s). The keys.txt already contains a couple of them, but more is better. You can generate the free keys by going to https://serpapi.com and registering for the free account (100 searches / month). Insert all available keys into the keys.txt file (new line for each key) and the tool will automatically choose the ones that have capacity.

## 1. Querying process

The platform discovery process begins by identifying parts of text or image annotations commonly found on sites using a particular technology of interest. These are usually phrases or (partial) HTML elements such as:

```bash
Powered by Socrata
```
```bash
"<meta name=\"generator\" content=\"ckan"
```

Having identified possible common _markers_, you can formulate queries and process results from the common search engines (SEs) with the tool. The tool extends the SERP API as a reliable solution to SE querying. The search could be possible with Google, Bing, Yandex, Yahoo, DuckDuckGo, Baidu and Naver, but only Google is currently implemented.

Normally, the search is defined by the _engine_ (e.g. Google), _query_ itself, _count_ (number of results per page, whereby you need to consider maximum allowed values for a given SE) and _offset_ (pagination, could be either pages - 1, 2, 3 - or skipped results - 100, 200, 300 - depending on the SE). Thus, each SE allows a different maximum amount of results per page and may vary in its approach to pagination. Below are the command templates for each SE.

After each search, the tool prints the actual number of usable results returned or an error when no results are available anymore. Normally, it makes sense to increase the pagination until the results are exhausted. At the same time, the number of results gives a good estimation of how well the platforms are discoverable with the given query (more general queries lead to more results, but often less hits, and more specific queries to less results, but a larger proportion of hits).

The search results are aggregated in the _./results_ folder as JSON files. These results files are also used as input files in the _portal\_handler_ script in the _data\_portal\_tracker_ project directory, which extracts all of the organic result URLs and adds them to the list creation and portal validation pipeline.

Google:
```bash
python3 crawley-lite.py --query "{QUERY}" --engine Google --count {COUNT}  --offset {OFFSET}
```

The maximum number of results for Google is 100 (i.e. _count_ set to 100) and _offset_ is calculated in total results to be skipped (i.e. 0,100,200, ... if _count_ is set to 100).

```bash
python3 crawley-lite.py --query "QUERY" --engine Google --count 100  --offset 100
```

Notes: Optimal queries with Google include _exact match searches_ (terms in parentheses), _inurl:_ and _site:_ operators (https://ahrefs.com/blog/google-advanced-search-operators/) and exclusions of clear false positives through _-site:_

```bash
python3 crawley-lite.py --query "site:*.socrata.com \"Open Data Portal\" -site:socrata.com" --engine Google --count 100  --offset 0
```

However, such specific queries often don't have any organic results. Here's a simple, definitely successful query to copy and try out:

```bash
python3 crawley-lite.py --query "Open Data Portal" --engine Google --count 100  --offset 0
```

The command to get the next page of results would be (_offset_ increased by 100):

```bash
python3 crawley-lite.py --query "Open Data Portal" --engine Google --count 100  --offset 100
```

## 2. Validating the platforms

The core part of the validation code was moved to the _portal\_handler_ script in the _data\_portal\_tracker_ directory and extended with additional API verification capabilities. Results of the marker-based validation, which were previously found in _validatedSites.json_, can now be output to any user-specified JSON file in the _data\_portal\_tracker_ directory. In contrast, the _config.json_ for defining the validation rules remains in the _crawley-lite_ directory.

## 3. Following links

This feature was removed for now, as the core search engine crawling and portal validation functionality are more important for the _data\_portal\_tracker_ project.

## Contributing

Contributions are welcome. Please open an issue or submit a pull request if you would like to contribute.

## License

The tool is avaiable under the [CC-BY 4.0 license](http://creativecommons.org/licenses/by/4.0/)

## Authors

- [Daniil Dobriy](https://github.com/semantisch) (Institute for Data, Process and Knowledge Management - WU Vienna) - created the tool.

*[Institute for Data, Process and Knowledge Management](https://www.wu.ac.at/en/dpkm/)*, *[Vienna University of Economics and Business](https://www.wu.ac.at)*.

- [Florian Jauernig](https://github.com/FlorianJauernig) - adapted code and readme for use with the _data\_portal\_tracker_ project.

## Acknowledgements
- Martin Beno and Alexandra Hager
- [Distributed Knowledge Graphs COST Action](https://cost-dkg.eu)
- [Diátaxis: A systematic framework for technical documentation authoring](https://diataxis.fr)