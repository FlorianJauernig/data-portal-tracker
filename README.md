# Data Portal Tracker
As a successor to _Open Data Portal Watch_, the Data Portal Tracker aims to semi-automatically create, validate and regularly update a comprehensive list of Open Data portals, crawl the URLs of all datasets and the associated metadata on these portals and add them to the _Open Dataset Archiver_ for downloading, periodic crawling and version tracking. It consists of the core _Data Portal Tracker_ functionality and an adapted version of Daniil Dobriy's search engine crawling tool _Crawley_ and connects to the _Open Dataset Archiver's_ API and MongoDB.

Currently supported search engine API: Google. <br>
Currently supported portal software: CKAN, Opendatasoft and Socrata.

## Install required packages
```bash
pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt
```

## Structure
| Path | Content |
| --- | --- |
| crawley-lite/crawley-lite.py | **Search engine portal discovery** functionality based on Crawley |
| data_portal_tracker/portal_handler.(ipynb\|py) | **Portal list creation and validation** pipeline |
| data_portal_tracker/portal_crawler.(ipynb\|py) | **Portal crawling** scripts |
| data_portal_tracker/archiver_connector.(ipynb\|py) | **Open Dataset Archiver connection** class and methods |
| data_portal_tracker/helpers.py | **Helper functions** for URL processing |
| data_portal_tracker/experiments.ipynb | **Experiments** that support implementation decisions and miscellaneous code |

## Documentation
Comprehensive documentation is provided here: **./Documentation.pdf**

More details on how to use Crawley can be found here: **crawley-lite/README.md**

## Results
Below are the results from the first portal discovery and validation run.

| Total URLs | Active sites | Inactive sites | Working APIs |
| --- | --- | --- | --- |
| 5539 | 4380 | 1159 | 705 |

| CKAN | Opendatasoft | Socrata|
| --- | --- | --- |
| 224 | 355 | 126 |

## Disclaimer
This is the first version of the Data Portal Tracker. Even though testing has shown no major issues, you should not blindly trust every result, but in case of doubt always perform some manual sanity checks. If you want to improve this tool and need a starting point, please see the limitations and future work chapters of my thesis for areas for improvement. The documentation in the annex also outlines the steps to add support for other portal software options.

## Related literature
Daniil Dobriy and Axel Polleres. Crawley: A Tool for Web Platform Discovery. In *Proceedings of the 22nd International Semantic Web Conference*, 2023.

Sebastian Neumaier, Jürgen Umbrich, and Axel Polleres. Automated Quality Assessment of Metadata across Open Data Portals. *Journal of Data and Information Quality*, 8(1):1–29, November 2016.

Thomas Weber, Johann Mitöhner, Sebastian Neumaier, and Axel Polleres. ODArchive – Creating an Archive for Structured Data from
Open Data Portals. In Jeff Z. Pan, Valentina Tamma, Claudia d’Amato, Krzysztof Janowicz, Bo Fu, Axel Polleres, Oshani Seneviratne, and Lalana Kagal, editors, *The Semantic Web – ISWC 2020*, volume 12507, pages 311–327. Springer International Publishing, Cham, 2020. Series Title: Lecture Notes in Computer Science.

## Acknowledgements
- Daniil Dobriy (_Crawley_)
- Horia-Stefan Dinu and Nicolas Ferranti (_Portalwatch\_API_)
- Martin Beno

## Relevant links
- Crawley: https://github.com/semantisch/crawley
- Open Dataset Archiver: https://archiver.ai.wu.ac.at