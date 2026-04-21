"""
rent_collector — official Israeli rental-price benchmark collector.

Fetches rent data from:
  - nadlan.gov.il  (primary: median rent by locality + room group)
  - CBS REST API   (secondary: average rent by city + room group)
  - CBS Table 4.9  (validation: average rent PDF/Excel)
  - BoI hedonic    (fallback model for localities with no data)
"""

__version__ = "0.1.0"
