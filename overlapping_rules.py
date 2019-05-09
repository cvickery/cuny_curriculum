""" Report rules that overlap one-another in their GPA requirements.
    Triggered by MA 114 from QCC to QNS, which says C- or below is blanket credit, but D or above
    transfers as MATH 115.
"""

import psycopg2
from psycopg2.extras import NamedTupleCursor

conn = psycopg2.connect('dbname=cuny_courses')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)
