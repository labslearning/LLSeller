"""
================================================================================
[GOD TIER OMEGA] COSMIC REPORT URL ROUTING
================================================================================
"""

from django.urls import path
from . import views_report

app_name = 'sales'

urlpatterns = [
    path(
        'cosmic-report/<uuid:inst_id>/',
        views_report.view_ai_report,
        name='cosmic_report'
    ),
    path(
        'cosmic-report/<uuid:inst_id>/<str:format_type>/',
        views_report.view_ai_report,
        name='cosmic_report_format'
    ),
]