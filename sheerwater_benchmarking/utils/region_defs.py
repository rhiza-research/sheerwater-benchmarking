# ruff: noqa: E501 <- line too long
"""Region definitions for the Sheerwater Benchmarking project.

This file contains several dictionaries, each representing an administrative level.

The keys are the names of the regions, and the values are dictionaries with the following keys:
- 'countries': a list of country names
- 'lons': a list of longitude bounds
- 'lats': a list of latitude bounds

The regions are defined as follows:
- countries
- regional_areas 
- continents
- meterological_zones
- hemispheres
- world
"""
# Individual countries of the world
countries = [
    'Indonesia', 'Malaysia', 'Chile', 'Bolivia', 'Peru', 'Argentina', 'Dhekelia Sovereign Base Area', 'Cyprus', 'India', 'China', 'Israel', 'Palestine', 'Lebanon', 'Ethiopia', 'South Sudan', 'Somalia', 'Kenya', 'Malawi', 'United Republic of Tanzania', 'Syria', 'Somaliland', 'France', 'Suriname', 'Guyana', 'South Korea', 'North Korea', 'Morocco', 'Western Sahara', 'Costa Rica', 'Nicaragua', 'Republic of the Congo', 'Democratic Republic of the Congo', 'Bhutan', 'Ukraine', 'Belarus', 'Namibia', 'South Africa', 'Saint Martin', 'Sint Maarten', 'Oman', 'Uzbekistan', 'Kazakhstan', 'Tajikistan', 'Lithuania', 'Brazil', 'Uruguay', 'Mongolia', 'Russia', 'Czechia', 'Germany', 'Estonia', 'Latvia', 'Norway', 'Sweden', 'Finland', 'Vietnam', 'Cambodia', 'Luxembourg', 'United Arab Emirates', 'Belgium', 'Georgia', 'North Macedonia', 'Albania', 'Azerbaijan', 'Kosovo', 'Turkey', 'Spain', 'Laos', 'Kyrgyzstan', 'Armenia', 'Denmark', 'Libya', 'Tunisia', 'Romania', 'Hungary', 'Slovakia', 'Poland', 'Ireland', 'United Kingdom', 'Greece', 'Zambia', 'Sierra Leone', 'Guinea', 'Liberia', 'Central African Republic', 'Sudan', 'Djibouti', 'Eritrea', 'Austria', 'Iraq', 'Italy', 'Switzerland', 'Iran', 'Netherlands', 'Liechtenstein', 'Ivory Coast', 'Republic of Serbia', 'Mali', 'Senegal', 'Nigeria', 'Benin', 'Angola', 'Croatia', 'Slovenia', 'Qatar', 'Saudi Arabia', 'Botswana', 'Zimbabwe', 'Pakistan', 'Bulgaria', 'Thailand', 'San Marino', 'Haiti', 'Dominican Republic', 'Chad', 'Kuwait', 'El Salvador', 'Guatemala', 'East Timor', 'Brunei', 'Monaco', 'Algeria', 'Mozambique', 'eSwatini', 'Burundi', 'Rwanda', 'Myanmar', 'Bangladesh', 'Andorra', 'Afghanistan', 'Montenegro', 'Bosnia and Herzegovina', 'Uganda', 'US Naval Base Guantanamo Bay', 'Cuba', 'Honduras', 'Ecuador', 'Colombia', 'Paraguay', 'Brazilian Island', 'Portugal', 'Moldova', 'Turkmenistan', 'Jordan', 'Nepal', 'Lesotho', 'Cameroon', 'Gabon', 'Niger', 'Burkina Faso', 'Togo', 'Ghana', 'Guinea-Bissau', 'Gibraltar', 'United States of America', 'Canada', 'Mexico', 'Belize', 'Panama', 'Venezuela', 'Papua New Guinea', 'Egypt', 'Yemen', 'Mauritania', 'Equatorial Guinea', 'Gambia', 'Hong Kong S.A.R.', 'Vatican', 'Northern Cyprus', 'Cyprus No Mans Area', 'Siachen Glacier', 'Baykonur Cosmodrome', 'Akrotiri Sovereign Base Area', 'Southern Patagonian Ice Field', 'Bir Tawil', 'Antarctica', 'Australia', 'Greenland', 'Fiji', 'New Zealand', 'New Caledonia', 'Madagascar', 'Philippines', 'Sri Lanka', 'Curaçao', 'Aruba', 'The Bahamas', 'Turks and Caicos Islands', 'Taiwan', 'Japan', 'Saint Pierre and Miquelon', 'Iceland', 'Pitcairn Islands', 'French Polynesia', 'French Southern and Antarctic Lands', 'Seychelles', 'Kiribati', 'Marshall Islands', 'Trinidad and Tobago', 'Grenada', 'Saint Vincent and the Grenadines', 'Barbados', 'Saint Lucia', 'Dominica', 'United States Minor Outlying Islands', 'Montserrat', 'Antigua and Barbuda', 'Saint Kitts and Nevis', 'United States Virgin Islands', 'Saint Barthelemy', 'Puerto Rico', 'Anguilla', 'British Virgin Islands', 'Jamaica', 'Cayman Islands', 'Bermuda', 'Heard Island and McDonald Islands', 'Saint Helena', 'Mauritius', 'Comoros', 'São Tomé and Principe', 'Cabo Verde', 'Malta', 'Jersey', 'Guernsey', 'Isle of Man', 'Aland', 'Faroe Islands', 'Indian Ocean Territories', 'British Indian Ocean Territory', 'Singapore', 'Norfolk Island', 'Cook Islands', 'Tonga', 'Wallis and Futuna', 'Samoa', 'Solomon Islands', 'Tuvalu', 'Maldives', 'Nauru', 'Federated States of Micronesia', 'South Georgia and the Islands', 'Falkland Islands', 'Vanuatu', 'Niue', 'American Samoa', 'Palau', 'Guam', 'Northern Mariana Islands', 'Bahrain', 'Coral Sea Islands', 'Spratly Islands', 'Clipperton Island', 'Macao S.A.R', 'Ashmore and Cartier Islands', 'Bajo Nuevo Bank (Petrel Is.)', 'Serranilla Bank', 'Scarborough Reef'
]

valid_regions = {
    'regional_areas': {
        'east_africa': {
            'countries': ['Kenya', 'Burundi', 'Rwanda', 'United Republic of Tanzania', 'Uganda'],
            'lons': [28.2, 42.6],
            'lats': [-12.1, 5.6]
        },
        'west_africa': {
            'countries': ['Benin', 'Burkina Faso', 'Cabo Verde', 'Ivory Coast', 'Gambia', 'Ghana', 'Guinea', 'Guinea-Bissau', 'Liberia', 'Mali', 'Mauritania', 'Niger', 'Nigeria', 'Senegal', 'Sierra Leone', 'Togo'],
            'lons': [0.0, 10.0],
            'lats': [4.0, 17.0]
        },
        'north_africa': {
            'countries': ['Algeria', 'Egypt', 'Libya', 'Morocco', 'Sudan', 'Tunisia'],
            'lons': [10.0, 30.0],
            'lats': [20.0, 35.0]
        },
        'central_africa': {
            'countries': ['Angola', 'Cameroon', 'Central African Republic', 'Chad', 'Republic of the Congo', 'Democratic Republic of the Congo', 'Equatorial Guinea', 'Gabon'],
            'lons': [10.0, 30.0],
            'lats': [0.0, 10.0]
        },
        'southern_africa': {
            'countries': ['Botswana', 'Lesotho', 'Namibia', 'South Africa', 'eSwatini', 'Zambia', 'Zimbabwe'],
            'lons': [10.0, 30.0],
            'lats': [-35.0, -10.0]
        },
        'conus': {
            'countries': ['United States of America'],
            'lons': [-125.0, -66.0],
            'lats': [24.0, 49.0]
        }
    },
    'continents': {
        'africa': {
            'countries': ['Algeria', 'Angola', 'Benin', 'Botswana', 'Burkina Faso', 'Burundi', 'Cameroon', 'Cabo Verde', 'Central African Republic', 'Chad', 'Comoros', 'Republic of the Congo', 'Democratic Republic of the Congo', 'Ivory Coast', 'Djibouti', 'Egypt', 'Equatorial Guinea', 'Eritrea', 'Ethiopia', 'Gabon', 'Gambia', 'Ghana', 'Guinea', 'Guinea-Bissau', 'Kenya', 'Lesotho', 'Liberia', 'Libya', 'Madagascar', 'Malawi', 'Mali', 'Mauritania', 'Mauritius', 'Morocco', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Rwanda', 'São Tomé and Principe', 'Senegal', 'Seychelles', 'Sierra Leone', 'Somalia', 'South Africa', 'South Sudan', 'Sudan', 'eSwatini', 'United Republic of Tanzania', 'Togo', 'Tunisia', 'Uganda', 'Zambia', 'Zimbabwe'],
            'lons': [-23.0, 58.0],
            'lats': [-35.0, 37.5]
        },
        'europe': {
            'countries': ['Albania', 'Andorra', 'Austria', 'Belarus', 'Belgium', 'Bosnia and Herzegovina', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kosovo', 'Latvia', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Monaco', 'Montenegro', 'Netherlands', 'North Macedonia', 'Norway', 'Poland', 'Portugal', 'Romania', 'Russia', 'San Marino', 'Republic of Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Ukraine', 'United Kingdom'],
            'lons': [-10.0, 45.0],
            'lats': [35.0, 72.0]
        },
        'asia': {
            'countries': ['Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Bhutan', 'Brunei', 'Cambodia', 'China', 'East Timor', 'Georgia', 'India', 'Indonesia', 'Iran', 'Iraq', 'Israel', 'Japan', 'Jordan', 'Kazakhstan', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Lebanon', 'Malaysia', 'Maldives', 'Mongolia', 'Myanmar', 'Nepal', 'Oman', 'Pakistan', 'Palestine', 'Philippines', 'Qatar', 'Saudi Arabia', 'Singapore', 'South Korea', 'Sri Lanka', 'Syria', 'Taiwan', 'Tajikistan', 'Thailand', 'East Timor', 'Turkey', 'Turkmenistan', 'United Arab Emirates', 'Uzbekistan', 'Vietnam', 'Yemen'],
            'lons': [60.0, 150.0],
            'lats': [0.0, 50.0]
        },
        'oceania': {
            'countries': ['Australia', 'Fiji', 'Kiribati', 'Marshall Islands', 'Federated States of Micronesia', 'Nauru', 'New Zealand', 'Palau', 'Papua New Guinea', 'Samoa', 'Solomon Islands', 'Tonga', 'Tuvalu', 'Vanuatu'],
            'lons': [110.0, 180.0],
            'lats': [-10.0, 10.0]
        },
        'north_america': {
            'countries': ['Canada', 'United States of America'],
            'lons': [-130.0, -67.0],
            'lats': [25.0, 50.0]
        },
        'south_america': {
            'countries': ['Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela'],
            'lons': [-85.0, -30.0],
            'lats': [-60.0, 15.0]
        },
        'antarctica': {
            'countries': ['Antarctica'],
            'lons': [-180.0, 180.0],
            'lats': [-90.0, 90.0]
        }
    },
    'meterological_zones': {
        'tropics': {
            'countries': [],
            'lons': [-180.0, 180.0],
            'lats': [-23.5, 23.5]
        },
        'extratropics': {
            'countries': [],
            'lons': [-180.0, 180.0],
            'lats': [-90.0, -23.5]
        }
    },
    'hemispheres': {
        'northern_hemisphere': {
            'countries': [],
            'lons': [-180.0, 180.0],
            'lats': [0.0, 90.0]
        },
        'southern_hemisphere': {
            'countries': [],
            'lons': [-180.0, 180.0],
            'lats': [-90.0, 0.0]
        }
    },
    'world': {
        'global': {
            'countries': [],
            'lons': [-180.0, 180.0],
            'lats': [-90.0, 90.0]
        }
    }
}
