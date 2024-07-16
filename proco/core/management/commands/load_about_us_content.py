import sys
import traceback

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.about_us.models import AboutUs

about_us_content_json = [
    {
        "text": [
            "An open & live global map of schools and their connectivity"
        ],
        "cta": {
            "link": [
                "/map"
            ],
            "text": [
                "Explore Giga Maps"
            ]
        },
        "content": [],
        "title": "Global school connectivity map",
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/6f068150-2292-4587-a50b-fdd70b8fc75d.png",
        "type": "live-map",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": {
            "link": [
                "https://giga.global/"
            ],
            "text": [
                "Know more about Giga"
            ]
        },
        "content": [
            {
                "text": [
                    "schools location mapped on Giga Maps out of 6M schools around the world"
                ],
                "title": "2.1/6M"
            },
            {
                "text": [
                    "schools reporting connectivity status"
                ],
                "title": "350k"
            },
            {
                "text": [
                    "schools transmitting real-time connectivity status"
                ],
                "title": "90.7K"
            }
        ],
        "title": "Giga connects every young person to information,",
        "style": None,
        "image": None,
        "type": "school-connected",
        "status": True,
        "order": 1
    },
    {
        "text": [
            "2.1M schools location collected from 50 government bodies, Open data sources like OSM and Giga's AI model.",
            "Locate schools through our open map"
        ],
        "cta": {
            "link": [
                "/map"
            ],
            "text": [
                "Explore school location layer"
            ]
        },
        "content": [],
        "title": "School Location",
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/2ab27c5e-b72e-4453-a609-35d6ba529016.png",
        "type": "school-location",
        "status": True,
        "order": 1
    },
    {
        "text": [
            "Real time connectivity monitored through GigaCheck Desktop App and chrome extension, Brazil's nic.br app and multiple ISP collaborations.",
            "Monitor real-time connectivity of schools"
        ],
        "cta": {
            "link": [
                "/map"
            ],
            "text": [
                "Explore Real-time connectivity layer "
            ]
        },
        "content": [],
        "title": "School Connectivity",
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/c9f1b133-dab0-4a21-abbf-fcfa53e299f7.png",
        "type": "school-connectivity",
        "status": True,
        "order": 1
    },
    {
        "text": [
            "Infrastructure data contributed by ~10 partners like ITU, Meta, GSMA, and government bodies.",
            "Understand connectivity infrastructure available around schools"
        ],
        "cta": {
            "link": [
                "/map"
            ],
            "text": [
                "Explore Coverage layer "
            ]
        },
        "content": [],
        "title": "Infrastructure",
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/f0644494-6b02-4e6c-b8e2-988dbae75733.png",
        "type": "infrastructure",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": [],
        "content": [
            {
                "text": [
                    "Former Deputy Minister of Public Education and current Deputy Minister of the Ministry of Digital Technologies, Republic of Uzbekistan.",
                    "“Giga’s real-time monitoring of schools’ Internet connectivity will help improve the efficiency of resource allocation for digital public goods and services and enhance accountability.”"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/b944d7cc-e956-47e9-9e71-bcca017c99c1.png",
                "title": "Rustam Karimjonov"
            },
            {
                "text": [
                    "Former UNICEF Uzbekistan representative. ",
                    "“Giga enabled the Government to define exact location and connectivity status of all state schools (10,132) of Uzbekistan, this was a first time this kind of data was collected and visualised in an open source manner.”"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/fe68e139-b209-436b-9e0a-a3b4a0994be8.jpg",
                "title": "Munir Mammadzade"
            },
            {
                "text": [
                    "Acting representative, UNICEF Kyrgyzstan.",
                    "“Giga helped to map out the level of connectivity in all 2,300 schools in Kyrgyzstan for the first time. It allowed the Government to make informed decisions on how much funding is needed to connect schools.”"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/2ea5da50-d764-4925-9da6-57d85269566a.png",
                "title": "Cristina Brugiolo"
            },
            {
                "text": [
                    "Minister of Communications and Transport of Bosnia and Herzegovina ",
                    "“Bosnia and Herzegovina is the first country in the region with a map of all schools and their connectivity status. Our children deserve the best, and we are committed to advancing school connectivity through Giga.”"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/8d8ca49b-9376-4ab5-b183-91313a7d5e4d.png",
                "title": "HE Edin Forto"
            }
        ],
        "title": None,
        "style": None,
        "image": None,
        "type": "gigamaps-enabled",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": {
            "link": [
                "https://#/map"
            ],
            "text": [
                "Gigablog"
            ]
        },
        "content": [
            {
                "text": [
                    "In Colombia, Giga leveraged advanced artificial intelligence techniques to automatically map schools from satellite imagery and provided the government with the locations of 7,000 schools that were previously omitted from their official school dataset."
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/d94da279-c947-4135-8774-baf8b8b5692e.png",
                "title": "Colombia"
            },
            {
                "text": [
                    "Giga helped the Kyrgyzstan Government save 40% (~$200k per year) of its education connectivity budget by revealing discrepancies in school connectivity through mapping. The information revealed in Giga maps led to renegotiations with suppliers for improved internet speed and lower costs for previously unconnected schools. "
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/88c786f5-4a70-465d-b91e-b358f34d8a98.png",
                "title": "Kyrgyzstan"
            },
            {
                "text": [
                    "The Organization for Eastern Caribbean States, comprising of nine island countries, is using school maps to inform the Children's Climate and Disaster Risk Model for better planning when disasters strike. The model uses the school location and connectivity data available in Giga Maps to improve decision-making for disaster communication, shelter and resource distribution during natural disasters. "
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/8dc4dadb-1389-4545-8c41-f3c4cbc99f8b.png",
                "title": "OECS"
            },
            {
                "text": [
                    "Giga used artificial intelligence to rectify and validate the locations of 20,000 schools, ensuring precision in mapping and correcting the geolocation of mislocated schools in their official dataset.   "
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/5a9b417f-6b56-4a3a-84c0-278f49bc6deb.jpg",
                "title": "Sudan"
            },
            {
                "text": [
                    "Giga facilitated the integration of school data from various sources, resulting in a comprehensive national school dataset. This foundational work established the basis for a robust multilayered infrastructure map, guiding the government in selecting optimal technologies for school connectivity. Ongoing analysis supports strategic ICT infrastructure development, laying the groundwork for compelling proposals aimed at attracting donors and investors. "
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/7c190b11-21cc-4691-a223-1cd7c207dc21.jpg",
                "title": "Benin"
            },
            {
                "text": [
                    "The comprehensive school mapping led by UNICEF unveiled the digital divide, prompting state-level support for comprehensive internet connectivity in education and collaboration among national stakeholders and the private sector."
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/e1c2d5b3-a71f-4780-a981-038f57f79068.jpg",
                "title": "Bosnia and Herzegovina"
            },
            {
                "text": [
                    "Recognizing its pivotal role in enhancing transparency and accountability, the Ministry of ICT and Innovation encouraged all initiatives involved in school connectivity projects to install Giga’s Daily Check App in their targeted schools. This will establish a unified and real-time school connectivity map, significantly elevating the value of monitoring efforts for all stakeholders involved. "
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/7ef08d36-2904-4916-b60d-9e629adc8377.jpg",
                "title": "Rwanda"
            },
            {
                "text": [
                    "Since 2020, Giga has partnered with UNICEF Niger and ANSI to map and connect schools in Niger, achieving a milestone in 2022 with 20,000 schools on Project Connect, incorporating school connectivity status data. This foundational work lays the groundwork for the implementation of unique school IDs, enhancing data management, and sets the stage for deploying the Daily Check App to monitor internet quality effectively. "
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/84ab385f-4e8c-4cba-aa8e-b7af4d8dec66.jpg",
                "title": "Niger"
            },
            {
                "text": [
                    "In 2023, Giga provided technical support to UNICEF Sao Tome and Principe and played a pivotal role in securing funding for the first national school mapping exercise. This effort ensured the collection of crucial data that will inform the evolving national EMIS platform and will support the deployment of the Daily Check App, and the identification of effective connectivity solutions for unconnected schools."
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/dc1135b1-a01f-43a6-b998-d613cd8edcb5.png",
                "title": "Sao Tome and Principe"
            }
        ],
        "title": None,
        "style": None,
        "image": "images/aa792184-c9d7-429c-9d03-ec75fbe9d848.png",
        "type": "slides",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": [],
        "content": [
            {
                "cta": {
                    "link": [
                        "docs/explore-api"
                    ]
                },
                "text": [
                    "Request access to school location and real-time connectivity data."
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/c8e67e6f-2e06-46b2-bbc8-c6fc5fc27598.png",
                "title": "Data downloads & API"
            },
            {
                "text": [
                    "Explore and contribute the open-source code of GigaMaps webapp."
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/ea4fe6d0-f4b7-428e-9f45-4e97eff80eb8.png",
                "title": "Open-source code"
            },
            {
                "cta": {
                    "link": [
                        "https://projectconnect.unicef.org/country-progress"
                    ]
                },
                "text": [
                    "Track the progress of countries in terms of mapping schools and their connectivity"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/70d33b40-60e9-4ccb-8944-c90e998345f4.png",
                "title": "Country progress dashboard"
            },
            {
                "cta": {
                    "link": [
                        "https://projectconnect.unicef.org/daily-check-app"
                    ]
                },
                "text": [
                    "The desktop application installed by schools to monitor the quality of their internet connectivity"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/fea6763c-e451-4d7b-a10c-b3642dca1e36.png",
                "title": "Daily check app"
            },
            {
                "cta": {
                    "link": [
                        "https://giga.global/isps/"
                    ]
                },
                "text": [
                    "We invite Internet Service Providers to contribute in shaping the future of school connectivity"
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/7ef805a3-a38e-4a2b-9d5f-8fec85f938ab.png",
                "title": "GigaISP"
            },
            {
                "text": [
                    "Share and update school information including their location, infrastructure and connectivity."
                ],
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/ee74296d-2888-438b-a271-bbd68da84154.png",
                "title": "Gigasync"
            }
        ],
        "title": None,
        "style": None,
        "image": None,
        "type": "resources",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": [],
        "content": [
            {
                "text": [
                    "Giga Maps aims to build a global, dynamic map that showcases the status of school connectivity in the world. This tool helps to identify gaps in school connectivity, create transparency, estimate the capital investment needed to connect unconnected schools, develop plans, attract the necessary funding, and track progress towards achieving universal school Internet connectivity."
                ],
                "title": "What is Giga Maps?"
            },
            {
                "text": [
                    "Giga is a UNICEF-ITU global initiative to connect every school to the Internet and every young person to information, opportunity, and choice."
                ],
                "title": "What is Giga?"
            },
            {
                "text": [
                    "Mapping shows where resources are needed: comprehensive school location data allows to identify where resources are needed, enabling effective service delivery and resource allocation by governments, NGOs, and private sector actors, thus facilitating resource provision to communities.    It shows where there is internet and if it is reliable: While governments and organizations strive to connect schools to the internet, mapping allows to monitor both the status and the quality of connectivity in schools.   It improves access to data for good: comprehensive information on school locations enables better resource allocation and informed decision-making, beyond provision of connectivity, such as education planning, infrastructure development, and emergency response.   It highlights gaps in infrastructure: mapping reveals gaps in infrastructure and enables governments and investors to estimate the costs and requirements for extending connectivity, thus facilitating more informed and less risky financing decisions.   It establishes market demand: mapping can help revealing potential unmet demand to investors and internet service providers. "
                ],
                "title": "Why mapping schools and their connectivity status? "
            },
            {
                "text": [
                    "Giga’s school mapping work is guided by the core belief that data is a public good and that data can improve the delivery of critical services and resources for children and their communities. Just like knowing the location of health centres or government buildings, school locations are vital information for communities, making it a fundamental public good. As such, in many countries worldwide, school locations are already available to the public through platforms like Google Maps, 2GIS, and OpenStreetMaps. Giga has also found that making school locations and their connectivity status publicly accessible enhances transparency, promotes accountability, encourages responsible oversight and decision-making. This transparency, in turn, allows to identify untapped market demand and builds trust among stakeholders, thereby attracting investments for school connectivity."
                ],
                "title": "Why make school location and connectivity status data public?"
            },
            {
                "text": [
                    "Giga collects and stores various school data points, as outlined in the School Data Schema. Giga strongly believes that school data is a public good and aims to provide information that can have a positive impact without putting children at risk.   Therefore, some of these school data points are shared by Giga with the public via Giga Maps and are subject to classification under a Creative Commons Attribution 4.0 International license (CC BY 4.0), enabling their distribution as publicly accessible information.    However, Giga recognizes that in certain contexts, some school data might be too sensitive to share publicly. In such cases, Giga, in cooperation with the country's government, assess and implement measures to mitigate risks, and determine which data can be safely shared under a CC BY 4.0 licence and which data should instead remain confidential. "
                ],
                "title": "What is Giga’s policy on data sharing and licensing? "
            },
            {
                "text": [
                    "Giga has developed a data sharing framework centered around three core principles:   Public data gathered with public money creates public goods. School location data is a public good. Child protection should always be prioritized. The data sharing framework has been developed to make sure that the broader connectivity community can benefit from the data, giving priority to child protection and data privacy. We will also continuously explore synergies across programmes, such as Social Policy for example, on a case-by-case basis."
                ],
                "title": "What about data privacy? "
            },
            {
                "text": [
                    "Giga collects and stores various school data points,... "
                ],
                "title": "What type of data does Giga Maps show? "
            },
            {
                "text": [
                    "Giga welcomes participation from any country interested in advancing school connectivity. National governments are vital partners in this endeavor and must commit to collaboration. Therefore, national governments are required to provide political support, share data, and ensure equitable access to connectivity for all. Typically, Giga operates as a tripartite partnership involving UNICEF Country Offices, the International Telecommunication Union (ITU), and relevant government ministries, often both the Ministry of ICT and Ministry of Education. Initial interest in Giga in a country is usually conveyed through the UNICEF Country Office or to ITU regional Team directly to the Giga UNICEF Global team. "
                ],
                "title": "How can a country join the Giga initiative? "
            }
        ],
        "title": "FAQs",
        "style": None,
        "image": None,
        "type": "faqs",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": [],
        "content": [
            {
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/956b523b-9698-4809-9cde-e1a72875f5df.png"
            },
            {
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/5a78c24c-89d7-42c3-9cb7-ddb618949201.png"
            },
            {
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/e2b3e7b6-1659-41e9-b28b-a2cdc105e65b.png"
            },
            {
                "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/dbc2be2b-d91a-430c-a2cf-f40676db2e8c.png"
            },
            {
                "image": "images/b8cab889-6a83-482c-971a-1df885ec5847.png"
            },
            {
                "image": "images/b5940996-2cf9-479f-a759-285df106d803.png"
            },
            {
                "image": "images/91e475de-ab38-4292-832c-7576952648fb.png"
            },
            {
                "image": "images/b99c6336-4cc6-4c3c-b898-c84e09fdc9f3.png"
            },
            {
                "image": "images/aa207d38-917c-4cff-bd18-7d763f1ebb13.png"
            },
            {
                "image": "images/9cc0b8bc-e64b-4943-994a-336ca79599f5.png"
            },
            {
                "image": "images/b2a4bfb0-091f-42a0-b09d-c87a78ec549b.png"
            },
            {
                "image": "images/649620bb-72f6-471e-a63e-f32593c1af6b.png"
            },
            {
                "image": "images/5f226195-ae63-4b0a-ae00-fa696d705208.png"
            },
            {
                "image": "images/720399de-8e60-47f3-a850-cf29cab36346.png"
            },
            {
                "image": "images/143f9042-73bf-4efa-8648-9b09720b248b.png"
            }
        ],
        "title": "Giga partners",
        "style": None,
        "image": None,
        "type": "partners",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": [],
        "content": [
            {
                "image": "images/6fc3f0df-7dca-478c-b3c7-b5ac5faba85c.png"
            },
            {
                "image": "images/a8021023-0d0c-40ab-876d-68f9f35f041b.png"
            },
            {
                "image": "images/c139b94c-4fd8-4fe6-9cde-67ddc44ad180.png"
            },
            {
                "image": "images/c24a6ae8-d339-4a21-a7c6-9210b55063e5.png"
            },
            {
                "image": "images/59cc4de9-ed38-40ab-a988-7b7f75208119.png"
            }
        ],
        "title": "Acknowledgement",
        "style": None,
        "image": None,
        "type": "eleventh",
        "status": True,
        "order": 1
    },
    {
        "text": [
            "An open & live global map of schools and their connectivity"
        ],
        "cta": {
            "link": [
                "/about",
                "/map"
            ],
            "text": [
                "Get in touch",
                "Explore Giga Maps"
            ]
        },
        "content": [],
        "title": "Global School connectivity map",
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/6f068150-2292-4587-a50b-fdd70b8fc75d.png",
        "type": "live-map-get-in-touch",
        "status": True,
        "order": 1
    },
    {
        "text": [],
        "cta": {
            "link": [
                "/map"
            ],
            "text": [
                "Explore Giga Maps"
            ]
        },
        "content": [
            {
                "name": "About",
                "target": "live-map,school-connected,school-location,school-connectivity,infrastructure"
            },
            {
                "name": "Impact",
                "target": "gigamaps-enabled,slides"
            },
            {
                "name": "Resources",
                "target": "resources"
            },
            {
                "name": "FAQs",
                "target": "faqs"
            },
            {
                "name": "Partners",
                "target": "partners,eleventh"
            },
            {
                "name": "Get in touch",
                "target": "live-map-get-in-touch"
            }
        ],
        "title": None,
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/eb6bc959-332f-4b22-9dab-18e3c436b95b.png",
        "type": "header",
        "status": True,
        "order": 1
    },
    {
        "text": [
            "Connecting every young person to information, opportunity and choice."
        ],
        "cta": [],
        "content": {
            "footerLogo": [
                {
                    "text": "<a href=\"https://giga.global/\" class=\"giga-logo\" target=\"_blank\" rel=\"noreferrer\">giga</a>"
                },
                {
                    "text": "<a href=\"https://www.unicef.org/\" target=\"_blank\" rel=\"noreferrer\"><svg xmlns=\"http://www.w3.org/2000/svg\" width=\"48\" height=\"20\" viewBox=\"0 0 68 17\"><g fill=\"currentColor;\" fill-rule=\"evenodd\"><path d=\"M58.637.456c-3.502 0-6.351 2.849-6.351 6.351s2.849 6.35 6.35 6.35c3.502 0 6.351-2.848 6.351-6.35S62.137.456 58.637.456m4.953 2.806c-.324.22-.667.419-1.022.593a6.62 6.62 0 0 0-.758-1.187c.26-.16.51-.336.74-.53.393.33.742.708 1.04 1.124m-2.672-2.106a6.1 6.1 0 0 1 1.424.816 5.729 5.729 0 0 1-.7.494 6.705 6.705 0 0 0-1.046-.966c.116-.107.225-.22.322-.344m.493 1.442a5.96 5.96 0 0 1-1.07.447 9.359 9.359 0 0 0-.53-1.036c.205-.09.399-.202.58-.34.372.272.714.584 1.02.929m-.751-1.539c-.087.103-.18.199-.282.287a6.378 6.378 0 0 0-1.064-.588c.462.05.914.149 1.346.301m-.497.455a2.677 2.677 0 0 1-.487.272 10.058 10.058 0 0 0-.613-.853 6.38 6.38 0 0 1 1.1.58zM58.767.977c.234.282.454.583.655.898a2.602 2.602 0 0 1-.655.115zm0 1.276c.273-.013.54-.061.799-.148.194.323.368.662.52 1.01a5.825 5.825 0 0 1-1.32.183zM58.502.989V1.99a2.602 2.602 0 0 1-.646-.115c.195-.304.413-.601.646-.886m-.896.797a2.552 2.552 0 0 1-.488-.272 6.4 6.4 0 0 1 1.096-.579c-.217.27-.423.556-.608.851m.366-1.032a6.54 6.54 0 0 0-1.072.591 2.933 2.933 0 0 1-.282-.287c.437-.154.89-.254 1.354-.304m4.351 10.9c-.845-.463-.861-1.106-.816-1.342.044-.24.21-.135.315-.135.555 0 1.139-.165 1.859-.9.811-.828 1.066-2.668-.181-3.868-1.344-1.293-2.773-1.067-3.695.096-.189.238-.65.412-1.05.399-.59-.023-.217.443-.217.54 0 .097-.09.165-.143.15-.2-.057-.12.24-.12.33 0 .09-.082.135-.126.135-.249 0-.18.223-.173.285.007.06-.015.163-.098.209-.079.045-.172.24-.172.39 0 .27.247.457.66.765.412.308.464.6.472.81.008.209.027.548.12.772.112.27.097.765-.472.787-.697.028-1.934.584-2.039.622-.393.14-.913.19-1.318.088a6.136 6.136 0 0 1-.623-.505c-.081-.28.06-.58.202-.783.232.232.578.217.771.224.195.008 1.147-.14 1.282-.186.136-.045.195-.03.278 0 .363.132.861.171 1.064-.525.203-.697-.277-.502-.337-.472-.06.03-.143.022-.113-.037.09-.178-.044-.16-.12-.166-.194-.02-.449.188-.531.262-.083.076-.165.068-.196.053-.258-.129-1.028.057-1.267-.36.023-.112-.195-1.806-.306-2.099a.286.286 0 0 1 .029-.276c.195-.294.735.021 1.027.014.35-.01.406-.155.503-.273.091-.115.16-.05.21-.083.063-.041.003-.124.025-.169.024-.044.05-.022.109-.074.06-.054-.009-.165.022-.21.087-.13.322-.026.181-.367-.077-.187.03-.39.135-.525.17-.219.825-1.141-.33-2.1-1.102-.913-2.22-.84-2.94-.06-.718.78-.359 1.92-.284 2.22.075.3-.141.571-.39.69-.197.092-.646.392-.985.687a6.06 6.06 0 0 1 3.816-5.513c.096.121.205.236.322.344-.255.185-.808.68-.808.68s.076.011.146.028c.049.012.152.043.152.043s.488-.414.716-.58c.178.136.373.249.579.34-.11.187-.388.71-.388.71s.064.037.121.075c.058.04.095.073.095.073s.303-.57.417-.762c.255.085.52.134.79.146v1.046c-.15-.004-.533-.02-.689-.035l-.097-.01.054.081c.042.06.074.12.102.175l.01.022.026.003c.113.009.463.021.593.024v1.904h.265v-.435c.387-.019.709-.046 1.099-.08l.016-.002.342-.302-.186.016a14.27 14.27 0 0 1-1.1.097l-.171.01V3.558a6.12 6.12 0 0 0 1.425-.2c.127.315.369 1.077.369 1.077l.248-.078s-.241-.756-.367-1.07a6.136 6.136 0 0 0 1.14-.486c.292.36.544.75.752 1.167-.187.087-.529.208-.528.208.273.03.481.095.481.095s.097-.041.158-.069l.072.165.342.152-.04-.095-.14-.333c.362-.18.719-.384 1.059-.612a6.087 6.087 0 0 1-1.415 8.177zM51.71 2.144l-.098.088c-.373.33-1.224 1.2-1.093 2.155l.013.067.043.19.118-.11c.521-.476.912-1.284 1.072-2.217l.045-.262-.1.09m-1.728 1.649-.043.115c-.059.16-.166.495-.224.94-.08.63-.062 1.522.486 2.188l.06.079.064.08.081-.404c.09-.716-.029-2.215-.291-2.885l-.09-.227zm-.631 2.58-.008.117c-.045.642-.012 2.226 1.27 3.066l.152.098-.007-.18c-.022-.6-.723-2.395-1.25-3.038l-.149-.182zm.221 2.833.022.109c.187.91 1.05 2.384 2.383 2.811l.183.06-.066-.18c-.249-.68-1.533-2.267-2.359-2.79L49.55 9.1zm2.677-5.693-.106.064c-.992.606-1.456 1.34-1.421 2.245l.009.207.144-.12c.537-.445 1.208-1.58 1.406-2.225l.072-.235zm-.584 1.983-.075.07c-.255.243-1.077 1.117-1.005 2.199.01.167.049.345.112.531l.059.173.106-.149c.393-.553.914-2.012.885-2.69l-.008-.205zm.043 1.878-.057.083c-.414.615-.625 1.247-.613 1.83.012.43.15.842.41 1.225l.086.13.073-.138c.18-.336.321-1.428.3-2.292a3.02 3.02 0 0 0-.09-.73l-.054-.19zm.481 2.007-.027.088c-.124.399-.183.79-.173 1.161.025.84.395 1.496 1.106 1.951l.12.076.026-.162.01-.129c-.019-.682-.527-2.4-.926-2.926l-.11-.144zm-1.4 2.664.052.086c.535.895 1.441 2.051 3.137 1.754l.142-.024-.08-.119c-.317-.471-2.376-1.632-3.103-1.751l-.2-.033zm2.51 2.274-.267.072.256.105c1.078.441 2.43.572 3.214.308.294-.1.473-.262.692-.475 1.38.143 2.671 1.29 3.425 2.227l.04.046.059-.018a.874.874 0 0 0 .27-.189l.053-.057-.048-.063c-.786-1.027-2.016-1.665-2.07-1.69-1.107-.54-3.062-.961-5.625-.266\"></path><path d=\"m53.357 11.452.015.09c.161 1.017.574 2.35 2.29 2.202l.11-.009-.028-.107c-.116-.462-1.688-1.804-2.237-2.181l-.165-.085zm12.13-9.397.045.262c.16.933.552 1.74 1.073 2.218l.119.108.041-.19.014-.066c.13-.955-.72-1.826-1.093-2.155l-.099-.088zm1.787 1.625-.09.227c-.264.67-.384 2.17-.293 2.885l.081.405.064-.081.061-.079c.547-.666.566-1.558.485-2.188a4.458 4.458 0 0 0-.223-.94l-.043-.115zm.665 2.575-.15.182c-.526.643-1.228 2.437-1.249 3.039l-.007.18.15-.099c1.284-.84 1.316-2.424 1.272-3.066l-.01-.117zm-.19 2.845-.187.118c-.825.522-2.11 2.108-2.358 2.789l-.067.18.183-.06c1.333-.427 2.196-1.901 2.383-2.811l.024-.109.022-.108M64.994 3.42l.068.237c.184.648.833 1.796 1.361 2.251l.144.123.011-.207c.054-.904-.397-1.647-1.377-2.272l-.104-.066zm.563 2.007-.008.205c-.027.678.492 2.137.886 2.69l.106.149.059-.173a2.05 2.05 0 0 0 .111-.531c.072-1.082-.75-1.956-1.004-2.199l-.074-.07zm-.022 1.866-.056.19a2.97 2.97 0 0 0-.09.73c-.02.864.122 1.956.3 2.292l.074.137.088-.13a2.25 2.25 0 0 0 .406-1.224c.014-.583-.197-1.215-.612-1.83l-.055-.083zm-.455 2.004-.11.144c-.397.525-.906 2.244-.925 2.926l.01.129.027.162.12-.076c.71-.455 1.082-1.112 1.106-1.95a3.548 3.548 0 0 0-.174-1.162l-.028-.088-.027-.085m1.48 2.662-.199.033c-.727.119-2.786 1.28-3.103 1.751l-.08.119.142.024c1.697.297 2.602-.859 3.137-1.754l.051-.086zm-8.185 2.627c-.054.025-1.285.663-2.07 1.69l-.048.063.054.057c.055.06.184.16.27.19l.057.017.04-.046c.753-.937 2.045-2.084 3.425-2.227.219.213.398.375.694.475.783.264 2.134.133 3.214-.308l.255-.105L64 14.32c-2.564-.695-4.518-.274-5.625.266m5.581-3.224-.165.085c-.55.377-2.121 1.72-2.237 2.181l-.027.107.108.01c1.717.147 2.13-1.186 2.292-2.202l.015-.09zm-43.894-8.48h2.169V.904h-2.169zm.172 11.88h1.824V5.129h-1.824v9.635zM6.469 5.136h1.823v9.636H6.507V13.35H6.47c-.73 1.21-1.958 1.708-3.302 1.708C1.152 15.059 0 13.524 0 11.585V5.136h1.824v5.7c0 1.671.384 2.899 2.13 2.899.75 0 1.766-.384 2.15-1.381.344-.903.365-2.035.365-2.265zm5.464 1.42h.039c.613-1.209 1.957-1.708 2.917-1.708.671 0 3.646.173 3.646 3.263v6.66h-1.822V8.708c0-1.594-.672-2.457-2.208-2.457 0 0-.999-.058-1.766.71-.268.268-.767.69-.767 2.572v5.24h-1.824V5.136h1.785zm17.76 1.671c-.096-1.202-.667-2.04-1.964-2.04-1.734 0-2.402 1.504-2.402 3.756 0 2.25.668 3.758 2.402 3.758 1.2 0 1.944-.78 2.039-2.156h1.813c-.154 2.156-1.755 3.47-3.872 3.47-3.07 0-4.29-2.172-4.29-4.996 0-2.807 1.41-5.15 4.445-5.15 2.021 0 3.546 1.28 3.638 3.358zm4.611 2.058c-.133 1.679.553 3.416 2.402 3.416 1.411 0 2.116-.552 2.325-1.948h1.91c-.288 2.177-1.967 3.262-4.253 3.262-3.071 0-4.293-2.172-4.293-4.996 0-2.807 1.414-5.15 4.445-5.15 2.86.06 4.212 1.87 4.212 4.523v.893zm4.843-1.256c.039-1.602-.687-2.842-2.441-2.842-1.509 0-2.402 1.274-2.402 2.842zm3.958 5.701V6.415h-1.647v-1.26h1.647V3.096C43.165 1 44.745.447 46.1.447c.438 0 .86.113 1.3.188v1.506c-.309-.016-.613-.058-.92-.058-1.026 0-1.618.27-1.562 1.317v1.754h2.212v1.261h-2.212v8.315h-1.812\"></path></g></svg></a>"
                },
                {
                    "text": "<a href=\"https://www.itu.int/en/Pages/default.aspx\" target=\"_blank\" rel=\"noreferrer\"><svg xmlns=\"http://www.w3.org/2000/svg\" width=\"20\" height=\"20\" viewBox=\"0 0 37 40\"><g fill=\"currentColor;\" fill-rule=\"evenodd\"><path d=\"M17.886 18.362v.457c.385.229.77.457 1.156.61a20.653 20.653 0 0 0 4.085 1.6v-.381c-1.31-.381-2.62-.915-3.931-1.524a11.574 11.574 0 0 1-1.31-.762\"></path><path d=\"M36.075 14.78c-1.079-3.2-2.929-5.942-5.395-8.075h-.077a18.04 18.04 0 0 0-3.777-2.514 18.6 18.6 0 0 0-13.488-1.296c-2.08.61-4.007 1.524-5.703 2.82C6.325 3.884 5.09 1.98 3.78 0c.386.914 1.619 3.429 3.16 6.248-2.004 1.6-3.7 3.58-4.933 6.019-1.156 2.21-1.85 4.647-2.003 7.01v.837C-.15 26.895 3.627 33.524 10.178 36.8c4.393 2.21 9.403 2.514 14.105.99a18.466 18.466 0 0 0 2.852-1.142c3.082 1.295 6.242 2.361 9.248 3.352-2.466-1.219-5.318-2.59-7.861-4.19.54-.381 1.156-.762 1.695-1.22l.078-.076a18.483 18.483 0 0 0 4.701-5.866c.77-1.372 1.233-2.896 1.619-4.343v-.229c.077-.228.077-.38.154-.61v-.152c.462-2.819.231-5.714-.694-8.533zm-9.48-10.285c1.233.61 2.312 1.296 3.314 2.134-.462 0-1.002 0-1.541.076-.617-.61-1.31-1.143-2.158-1.524a9.764 9.764 0 0 0-2.235-.762c-.309-.457-.617-.914-.926-1.219 1.234.305 2.39.762 3.546 1.295M10.487 5.867c.154-.153.23-.381.308-.534 2.697-.61 5.78-.61 8.863.077-.077.076-.154.228-.23.304-.31.61-.31 1.296-.078 2.057.154.305.308.686.54.99a26.952 26.952 0 0 0-6.552 4.039 56.546 56.546 0 0 1-3.314-3.81c-.154-1.142 0-2.209.463-3.123M9.639 8.38c-.54-.686-1.08-1.371-1.542-2.057.694-.305 1.388-.61 2.158-.838 0 .076-.077.152-.077.228-.462.762-.616 1.676-.54 2.667zm10.096-.762c-.23-.61-.23-1.219.078-1.676l.23-.457c1.62.38 3.238.99 4.78 1.752-1.62.305-3.16.838-4.548 1.372-.231-.305-.462-.61-.54-.99zm5.087-.838a25.47 25.47 0 0 0-4.393-1.6c.154-.152.462-.229.694-.305.77-.305 1.618-.305 2.62-.152.386.686.771 1.447 1.08 2.057zm-.54-1.905c.54.153 1.157.381 1.773.61.694.38 1.31.762 1.773 1.219-.925.076-1.85.305-2.39.38h-.076c-.309-.685-.694-1.523-1.08-2.209m.849 2.667a24.143 24.143 0 0 1 1.618 4.495c-1.079 0-2.312-.305-3.468-.914-1.156-.534-2.158-1.295-2.775-2.134 1.465-.609 3.006-1.142 4.625-1.447m.539.076c1.465.762 2.775 1.676 4.008 2.667 0 .152-.077.38-.154.533-.231.457-.694.838-1.387 1.067-.309.076-.617.152-1.002.152-.309-1.448-.848-2.971-1.465-4.419m.308-.229c.617-.152 1.465-.228 2.235-.304.925.838 1.465 1.828 1.542 2.743-1.156-.915-2.39-1.753-3.777-2.439M13.492 3.276c2.852-.762 5.935-.914 8.941-.228.308.304.694.762 1.002 1.219-.925-.076-1.696 0-2.466.228-.386.153-.771.305-1.002.61a20.266 20.266 0 0 0-8.787-.229c.54-.685 1.31-1.219 2.312-1.6m-1.927.686c-.385.305-.693.686-1.001 1.067-.925.228-1.85.533-2.698.99a17.562 17.562 0 0 1 3.7-2.057zm-1.233 32.457c-1.233-.61-2.466-1.371-3.468-2.286-.54-1.371-.693-2.59-.848-3.885a29.956 29.956 0 0 0 5.627 3.733c2.158.99 4.316 1.752 6.397 2.21-.925.609-2.235 1.295-3.777 1.676a16.414 16.414 0 0 1-3.93-1.448zm13.797 1.067c-2.93.914-6.09 1.142-9.018.533 1.387-.457 2.62-1.143 3.391-1.6.077-.076.077-.076.154-.076 2.852.533 5.55.533 7.862 0 .077 0 .077.076.154.076-.77.381-1.619.762-2.543 1.067m-5.01-1.448c.925-.61 1.772-1.371 2.543-2.133 1.387.838 2.852 1.524 4.24 2.133-2.005.457-4.317.457-6.783 0m15.569-7.543a17.38 17.38 0 0 1-4.702 5.79c-.616.458-1.31.915-2.08 1.22a38.566 38.566 0 0 1-3.7-2.743c2.004-.457 3.545-1.143 5.395-2.286-.386-.152-.771-.305-1.156-.533a10.444 10.444 0 0 0 2.852-.61c1.85-.61 3.314-1.6 4.238-2.895-.23.686-.539 1.372-.847 2.057m1.695-5.257c-.077.229-.077.534-.154.762v.152c-.154.381-.308.839-.54 1.296-.77 1.6-2.389 2.819-4.47 3.504-1.079.381-2.158.61-3.391.686-.693-.38-1.464-.838-2.158-1.219l.463-1.143c-.078-.076-.232-.076-.309-.152-.154.38-.308.762-.54 1.066-2.543-1.523-5.01-3.428-7.398-5.409v2.59c1.31 1.143 2.62 2.21 3.853 3.277-.154.076-.385.228-.616.304-1.465-.38-2.929-.838-4.394-1.447h-.924c1.541.685 3.16 1.295 4.701 1.676-1.387.61-3.468 1.22-3.93 1.295a46.938 46.938 0 0 0 4.7 3.276c-.77.762-1.772 1.524-2.774 2.21a27.237 27.237 0 0 1-6.705-2.362 26.286 26.286 0 0 1-5.78-3.886 10.458 10.458 0 0 1 0-2.21H5.63c-.077.61-.077 1.22-.077 1.83-.617-.61-1.156-1.22-1.696-1.83h-.54c.617.839 1.465 1.6 2.313 2.363v.152c.077.99.154 2.21.616 3.58A18.253 18.253 0 0 1 .543 22.933c.463 1.143 1.08 2.362 1.927 3.505v-.076h.462C1.392 24.229.467 22.095.39 20.038c.077-2.59.693-5.18 1.927-7.619.385-.686.77-1.371 1.156-1.98-.386 1.142-.386 2.437-.154 3.732h.385c-.308-1.6-.154-3.123.54-4.419.23-.457.693-1.066 1.001-1.447.617-.61 1.234-1.067 1.927-1.524 1.388 2.438 2.852 4.952 4.162 6.629-1.233.076-2.003.076-3.237-.077.154.305.308.61.463.838l.154.381c.385.686.77 1.296 1.156 1.905a23.915 23.915 0 0 0-2.158 3.505v.914c.617-1.448 1.464-2.819 2.39-4.038l.076.076v-2.666h4.702c-.463-.381-.848-.762-1.233-1.143 1.85-1.6 4.084-2.972 6.474-3.962.694.914 1.695 1.752 2.928 2.362 1.31.61 2.621.99 3.777.914.154.61.309 1.22.386 1.829h.385c-.077-.61-.154-1.22-.308-1.829.308 0 .693-.076 1.002-.152.693-.305 1.31-.686 1.618-1.296 0-.152.077-.228.154-.38a25.764 25.764 0 0 1 3.469 3.657h.462c-1.08-1.448-2.39-2.896-3.854-4.115 0-.99-.462-2.057-1.387-3.123.617 0 1.233 0 1.696.076 2.235 1.904 3.93 4.343 5.01 7.162.23.914.23 1.828.076 2.59-.23-.457-.539-.99-.847-1.524v.153h-.386c.463.685.771 1.295 1.08 1.98-.078.23-.155.458-.309.686-.308.61-.77 1.143-1.387 1.6v.534c.77-.534 1.387-1.22 1.773-1.981.076-.077.076-.229.154-.305.693 1.752.924 3.505.77 5.257m-.616-5.79c.154-.458.231-.915.231-1.372.385 1.295.54 2.59.617 3.886-.232-.838-.463-1.676-.848-2.514\"></path><path d=\"M7.327 26.21V15.619c0-.457.539-.533 1.078-.533v-.534H2.78v.534c.54 0 1.002.076 1.002.533v10.59c0 .382-.385.458-.925.458v.457H8.33v-.457c-.617 0-1.002-.076-1.002-.457m13.178-7.848h.54v-3.733H10.564v3.733h.54c0-1.219.539-1.753 1.387-1.753h1.541v9.524c0 .229-.077.534-1.002.534v.457h5.55v-.457c-.925 0-1.08-.305-1.08-.534V16.61h1.619c.77 0 1.387.534 1.387 1.753m13.796-3.734h-4.007v.457c.539 0 1.078.076 1.078.61v7.77c0 1.144-1.31 1.753-2.312 1.753-1.079 0-2.312-.61-2.312-1.752v-7.772c0-.38.308-.61.77-.61v-.456h-4.855v.457c.463 0 .848.152.848.61v8.076c0 2.438 2.852 3.504 4.933 3.504 2.08 0 4.932-1.143 4.932-3.504v-8.077c0-.457.386-.61.925-.61z\"></path><path d=\"M17.886 17.448c.308-.077.616-.077.925-.153-.155-.152-.309-.228-.463-.38h-.462zm-4.162 1.6-1.08-1.067h1.08v-1.067H12.49c-.848 0-1.002.838-1.002 1.448v.305a37.168 37.168 0 0 0 2.235 2.666v-2.285m-.231 7.238c.155-.076.232-.076.232-.153v-.152c-2.39-1.371-4.393-3.048-6.012-4.724v.534c1.541 1.676 3.545 3.2 5.78 4.495m13.874-10.819c.23 1.98.23 3.961 0 5.866l-.077.381c-.077.381-.154.838-.232 1.22v.533c0 .228.078.38.155.533.23-.686.385-1.524.462-2.286 1.233.077 2.312-.076 3.314-.304v-.381c-1.002.228-2.08.38-3.237.304.231-1.905.231-3.885.077-5.866z\"></path></g></svg></a>"
                }
            ],
            "footerLinks": [
                {
                    "text": "<h4>Giga products</h4>\n<div class=\"footer-link-wrapper\">\n  <a href=\"/maps target=\"_blank\"\">Giga Maps</a>\n  <a href=\"https://projectconnect.unicef.org/daily-check-app\" target=\"_blank\">Daily check app</a>\n  <a href=\"https://giga.global/isps/\" target=\"_blank\">Giga ISP</a>\n  <a href=\"\" target=\"_blank\">Giga Sync </a>\n</div>"
                },
                {
                    "text": "<h4>Organization</h4>\n<div class=\"footer-link-wrapper\">\n<a href=\"https://giga.global/\" target=\"_blank\">Giga</a>\n<a href=\"https://www.unicef.org/\" target=\"_blank\">Unicef</a>\n<a href=\"https://www.itu.int/en/Pages/default.aspx\" target=\"_blank\">ITU</a>\n</div>"
                },
                {
                    "text": "<h4>Resources</h4>\n<div class=\"footer-link-wrapper\">\n  <a href=\"/docs/explore-api\" target=\"_blank\">Data downloads &amp; API</a>\n  <a href=\"\" target=\"_blank\">Open-source code</a>\n  <a href=\"https://projectconnect.unicef.org/country-progress\" target=\"_blank\">Country progress dashboard</a>\n  <a href=\"https://giga.global/stories/\" target=\"_blank\">Gigablog</a>\n</div>"
                }
            ],
            "socialLinks": [
                {
                    "text": "<a href=\"https://www.youtube.com/@gigaglobal\" target=\"_blank\" rel=\"noreferrer\">\n    <svg xmlns=\"http://www.w3.org/2000/svg\" width=\"32\" height=\"34\" fill=\"#fff\" viewBox=\"0 0 24 24\">\n        <path d=\"M12 2C6.475 2 2 6.475 2 12s4.475 10 10 10 10-4.475 10-10S17.525 2 12 2zm0 18c-4.418 0-8-3.582-8-8s3.582-8 8-8 8 3.582 8 8-3.582 8-8 8zM9.5 14.75v-5.5l6.25 2.75-6.25 2.75z\"/>\n    </svg>\n</a>"
                },
                {
                    "text": "<a href=\"https://twitter.com/Gigaglobal\" target=\"_blank\" rel=\"noreferrer\">\n   <svg xmlns=\"http://www.w3.org/2000/svg\" width=\"24\" height=\"24\" fill=\"none\" viewBox=\"0 0 1200 1227\"><g clip-path=\"url(#a)\"><path d=\"M714.163 519.284 1160.89 0h-105.86L667.137 450.887 357.328 0H0l468.492 681.821L0 1226.37h105.866l409.625-476.152 327.181 476.152H1200L714.137 519.284zM569.165 687.828l-47.468-67.894-377.686-540.24h162.604l304.797 435.991 47.468 67.894 396.2 566.721H892.476L569.165 687.854z\"></path></g><defs><clipPath id=\"a\"><path d=\"M0 0h1200v1227H0z\"></path></clipPath></defs></svg>\n</a>\n"
                },
                {
                    "text": "<a href=\"https://www.instagram.com/giga_global/\" target=\"_blank\" rel=\"noreferrer\"><svg xmlns=\"http://www.w3.org/2000/svg\" width=\"33\" height=\"34\" fill=\"none\" viewBox=\"0 0 33 34\"><path fill=\"#fff\" d=\"M0 0h32.522v32.522H0z\" transform=\"translate(.41 .885)\" style=\"mix-blend-mode: multiply;\"></path><path fill=\"#fff\" d=\"M23.182 12.1a1.463 1.463 0 1 0 0-2.928 1.463 1.463 0 0 0 0 2.927M16.672 10.883a6.263 6.263 0 1 0 0 12.526 6.263 6.263 0 0 0 0-12.526m0 10.328a4.066 4.066 0 1 1 0-8.132 4.066 4.066 0 0 1 0 8.132\"></path><path fill=\"#fff\" d=\"M16.672 7.148c3.257 0 3.642.012 4.928.07a6.75 6.75 0 0 1 2.265.42 4.04 4.04 0 0 1 2.314 2.315 6.75 6.75 0 0 1 .42 2.265c.059 1.286.072 1.672.072 4.928 0 3.256-.013 3.642-.072 4.928a6.749 6.749 0 0 1-.42 2.265 4.038 4.038 0 0 1-2.314 2.314 6.75 6.75 0 0 1-2.265.42c-1.286.059-1.671.071-4.928.071-3.256 0-3.642-.012-4.928-.07a6.75 6.75 0 0 1-2.264-.42 4.04 4.04 0 0 1-2.315-2.315 6.75 6.75 0 0 1-.42-2.265c-.059-1.286-.071-1.672-.071-4.928 0-3.256.012-3.642.071-4.928a6.75 6.75 0 0 1 .42-2.265A4.04 4.04 0 0 1 9.48 7.64a6.75 6.75 0 0 1 2.264-.42c1.286-.059 1.672-.071 4.928-.071m0-2.198c-3.312 0-3.727.014-5.028.074a8.95 8.95 0 0 0-2.96.566 6.236 6.236 0 0 0-3.567 3.568 8.95 8.95 0 0 0-.567 2.96c-.06 1.3-.073 1.716-.073 5.028 0 3.312.014 3.727.073 5.028a8.951 8.951 0 0 0 .567 2.96 6.236 6.236 0 0 0 3.567 3.568 8.948 8.948 0 0 0 2.96.567c1.3.059 1.716.073 5.028.073 3.312 0 3.728-.014 5.029-.074a8.949 8.949 0 0 0 2.96-.567 6.236 6.236 0 0 0 3.567-3.567 8.95 8.95 0 0 0 .567-2.96c.059-1.3.073-1.716.073-5.028 0-3.312-.014-3.728-.073-5.028a8.95 8.95 0 0 0-.567-2.96A6.236 6.236 0 0 0 24.66 5.59a8.951 8.951 0 0 0-2.96-.567c-1.3-.06-1.716-.074-5.028-.074\"></path></svg></a>"
                },
                {
                    "text": "<a href=\"https://www.linkedin.com/showcase/gigaglobal\" target=\"_blank\" rel=\"noreferrer\"><svg xmlns=\"http://www.w3.org/2000/svg\" width=\"33\" height=\"34\" fill=\"none\" viewBox=\"0 0 33 34\"><path fill=\"#fff\" d=\"M0 0h31.565v32.522H0z\" transform=\"translate(.934 .885)\" style=\"mix-blend-mode: multiply;\"></path><path fill=\"#fff\" d=\"M26.777 4.95H6.654c-.986 0-1.775.813-1.775 1.728v20.834c0 .915.789 1.728 1.775 1.728h20.123c.987 0 1.776-.813 1.776-1.728V6.678c0-.915-.79-1.728-1.776-1.728M11.882 25.683H8.43V14.097h3.452zM10.206 12.47c-1.086 0-2.072-.915-2.072-2.134 0-1.22.888-2.135 2.072-2.135 1.085 0 2.071.915 2.071 2.135 0 1.22-.986 2.134-2.071 2.134M25.1 25.58h-3.452V19.89c0-1.321 0-3.15-1.874-3.15s-2.072 1.524-2.072 2.947v5.793H14.25V14.097h3.255v1.524h.099c.493-.914 1.677-1.93 3.353-1.93 3.552 0 4.242 2.438 4.242 5.59v6.3z\"></path></svg></a>"
                }
            ],
            "footerDescription": "A United Nation's initiative"
        },
        "title": None,
        "style": None,
        "image": "https://saunigigaweb.blob.core.windows.net/giga-maps-backend-stg-public/images/540c86d3-1d0c-49dd-8c29-f730a04b4ef0.png",
        "type": "footer",
        "status": True,
        "order": 1
    }
]


def load_data_sources_data():
    try:
        AboutUs.objects.all().delete()
        sys.stdout.write('\nDelete all old record')
    except:
        print(traceback.format_exc())

    for row_data in about_us_content_json:
        try:
            instance, created = AboutUs.objects.update_or_create(**row_data)
            if created:
                sys.stdout.write('\nNew About Us content created: {}'.format(instance.__dict__))
            else:
                sys.stdout.write('\nExisting About Us content updated: {}'.format(instance.__dict__))
        except:
            print(traceback.format_exc())


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--load_about_us_content', action='store_true', dest='load_about_us_content', default=False,
            help='load_about_us_content'
        )

    def handle(self, **options):
        sys.stdout.write('\nLoading About US data....')

        with transaction.atomic():
            if options.get('load_about_us_content', False):
                load_data_sources_data()

        sys.stdout.write('\nAbout Us content loaded successfully!\n')
