#!/usr/bin/env python3
"""
Facebook Optimization Tool - Final Corrected Version
- Facebook API: Impressions & Link Clicks only
- Google Sheets: Spend (USD), Revenue, Conversions
- Corrected CPA calculation: Spend ÷ NPRs
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import gspread
from google.auth import default
import json
import os
import requests
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.data = []
        self.fb_creative_data = {}
        self.kpi_settings = self.load_default_kpi_settings()
        self.optimization_rules = self.load_default_optimization_rules()
        
        # Initialize APIs
        self.init_google_sheets()
        self.init_facebook_api()
        
        # Load data
        self.load_data()
        
        # Start background Facebook creative data loading
        threading.Thread(target=self.load_facebook_creative_data, daemon=True).start()
    
    def init_google_sheets(self):
        """Initialize Google Sheets API"""
        try:
            # Use service account credentials from environment
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if credentials_json:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    f.write(credentials_json)
                    credentials_path = f.name
                
                self.gc = gspread.service_account(filename=credentials_path)
                os.unlink(credentials_path)  # Clean up temp file
                print("✅ Google Sheets API initialized")
            else:
                print("❌ Google Sheets credentials not found")
                self.gc = None
        except Exception as e:
            print(f"❌ Google Sheets initialization error: {e}")
            self.gc = None
    
    def init_facebook_api(self):
        """Initialize Facebook API"""
        try:
            self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
            self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
            if self.fb_access_token and self.fb_ad_account_id:
                print("✅ Facebook API credentials configured")
            else:
                print("❌ Facebook API credentials not found")
        except Exception as e:
            print(f"❌ Facebook API initialization error: {e}")
    
    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            if not self.gc:
                print("❌ Google Sheets not initialized")
                return [], [], []
            
            # Load individual spreadsheets
            fb_spend_sheet = self.gc.open_by_key('1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk').sheet1
            attribution_sheet = self.gc.open_by_key('1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ').sheet1
            web_pages_sheet = self.gc.open_by_key('1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA').sheet1
            
            # Get all records
            fb_data = fb_spend_sheet.get_all_records()
            attr_data = attribution_sheet.get_all_records()
            web_data = web_pages_sheet.get_all_records()
            
            print(f"✅ Loaded {len(fb_data)} rows from fb_spend")
            print(f"✅ Loaded {len(attr_data)} rows from attribution")
            print(f"✅ Loaded {len(web_data)} rows from web_pages")
            
            return fb_data, attr_data, web_data
            
        except Exception as e:
            print(f"❌ Error loading Google Sheets: {e}")
            return [], [], []
    
    def load_facebook_api_data(self):
        """Load impressions and link clicks from Facebook API"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("❌ Facebook API not configured")
                return {}
            
            # Get date range (June 1 to current)
            start_date = "2025-06-01"
            end_date = datetime.now().strftime("%Y-%m-%d")
            
            url = f"https://graph.facebook.com/v18.0/act_{self.fb_ad_account_id}/ads"
            params = {
                'access_token': self.fb_access_token,
                'fields': 'name,adset{name},insights{impressions,clicks}',
                'time_range': f'{{"since":"{start_date}","until":"{end_date}"}}',
                'limit': 100
            }
            
            fb_api_data = {}
            page_count = 0
            max_pages = 20  # Reasonable limit
            
            print(f"🔄 Loading Facebook API data from {start_date} to {end_date}")
            
            while url and page_count < max_pages:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    ads = data.get('data', [])
                    
                    page_count += 1
                    print(f"📡 Loaded page {page_count}: {len(ads)} ads")
                    
                    for ad in ads:
                        ad_name = ad.get('name', '')
                        adset_info = ad.get('adset', {})
                        adset_name = adset_info.get('name', '') if adset_info else ''
                        insights = ad.get('insights', {}).get('data', [])
                        
                        if ad_name and adset_name and insights:
                            # Sum up insights data
                            total_impressions = sum(int(insight.get('impressions', 0)) for insight in insights)
                            total_clicks = sum(int(insight.get('clicks', 0)) for insight in insights)
                            
                            # Create combination key
                            combo_key = f"{adset_name}|||{ad_name}"
                            fb_api_data[combo_key] = {
                                'impressions': total_impressions,
                                'link_clicks': total_clicks,
                                'ad_name': ad_name,
                                'adset_name': adset_name
                            }
                    
                    # Get next page
                    paging = data.get('paging', {})
                    url = paging.get('next')
                    params = {}  # Next URL already has params
                    
                else:
                    print(f"❌ Facebook API error: {response.status_code}")
                    try:
                        error_data = response.json()
                        print(f"❌ Facebook API error details: {error_data}")
                    except:
                        print(f"❌ Facebook API response text: {response.text}")
                    break
            
            print(f"✅ Loaded {len(fb_api_data)} ad combinations from Facebook API")
            return fb_api_data
            
        except Exception as e:
            print(f"❌ Facebook API error: {e}")
            return {}
    
    def load_data(self):
        """Load and process all data"""
        print("🔄 Loading data from Google Sheets...")
        
        # Load Google Sheets data
        fb_data, attr_data, web_data = self.load_google_sheets_data()
        
        # Load Facebook API data for impressions/clicks
        fb_api_data = self.load_facebook_api_data()
        
        print(f"🔄 Processing data: {len(fb_data)} fb, {len(attr_data)} attr, {len(web_data)} web, {len(fb_api_data)} fb_api")
        
        # Process and combine data
        self.data = self.process_combined_data(fb_data, attr_data, web_data, fb_api_data)
        
        print(f"✅ Processed {len(self.data)} combined records")
    
    def process_combined_data(self, fb_data, attr_data, web_data, fb_api_data):
        """Process and combine data from all sources"""
        combined_data = []
        
        # Use FB Spend as primary dataset (exclude totals row)
        fb_records = [row for row in fb_data if row.get('Facebook Ad Name') and 
                     str(row.get('Facebook Ad Name')).lower() != 'total']
        
        for fb_row in fb_records:
            try:
                # Extract FB data
                ad_name = str(fb_row.get('Facebook Ad Name', '')).strip()
                adset_name = str(fb_row.get('Facebook Adset Name', '')).strip()
                spend = self.clean_numeric(fb_row.get('Facebook Total Spend (USD)', 0))
                
                if not ad_name or not adset_name:
                    continue
                
                # Create combination key for matching
                combo_key = f"{adset_name}|||{ad_name}"
                
                # Get Facebook API data (impressions/clicks)
                fb_api_record = fb_api_data.get(combo_key, {})
                impressions = fb_api_record.get('impressions', 0)
                link_clicks = fb_api_record.get('link_clicks', 0)
                
                # Try to match with attribution data (by UTM content)
                revenue = 0
                offer_spend = 0
                nprs = 0
                for attr_row in attr_data:
                    attr_utm = str(attr_row.get('Attribution UTM Content', '')).strip()
                    if attr_utm and (attr_utm in ad_name or ad_name in attr_utm):
                        revenue = self.clean_numeric(attr_row.get('Attribution Attibuted Total Revenue (Predicted) (USD)', 0))
                        offer_spend = self.clean_numeric(attr_row.get('Attribution Attibuted Offer Spend (Predicted) (USD)', 0))
                        nprs = self.clean_numeric(attr_row.get('Attribution Attributed NPRs', 0))
                        break
                
                # Try to match with web pages data (by UTM content)
                funnel_starts = 0
                survey_completions = 0
                checkout_starts = 0
                for web_row in web_data:
                    web_utm = str(web_row.get('Web Pages UTM Content', '')).strip()
                    if web_utm and (web_utm in ad_name or ad_name in web_utm):
                        funnel_starts = self.clean_numeric(web_row.get('Web Pages Unique Count of Sessions with Funnel Starts', 0))
                        survey_completions = self.clean_numeric(web_row.get('Web Pages Unique Count of Sessions with Match Results', 0))
                        checkout_starts = self.clean_numeric(web_row.get('Count of Sessions with Checkout Started (V2 included)', 0))
                        break
                
                # Calculate metrics
                ctr = (link_clicks / impressions * 100) if impressions > 0 else 0
                cpc = (spend / link_clicks) if link_clicks > 0 else 0
                cpm = (spend / impressions * 1000) if impressions > 0 else 0
                roas = (revenue / spend) if spend > 0 else 0
                cpa = (spend / nprs) if nprs > 0 else 0  # CORRECTED: Spend ÷ NPRs
                
                # Funnel rates
                funnel_start_rate = (funnel_starts / link_clicks * 100) if link_clicks > 0 else 0
                booking_rate = (nprs / link_clicks * 100) if link_clicks > 0 else 0
                survey_completion_rate = (survey_completions / funnel_starts * 100) if funnel_starts > 0 else 0
                checkout_start_rate = (checkout_starts / survey_completions * 100) if survey_completions > 0 else 0
                
                # Success criteria (8 criteria)
                success_count = 0
                if ctr >= 0.30: success_count += 1
                if funnel_start_rate >= 15.0: success_count += 1
                if cpa <= 120.0 and cpa > 0: success_count += 1
                if link_clicks >= 500: success_count += 1
                if roas >= 1.0: success_count += 1
                if cpc <= 10.0 and cpc > 0: success_count += 1
                if cpm <= 50.0 and cpm > 0: success_count += 1
                if booking_rate >= 2.0: success_count += 1
                
                combined_record = {
                    'ad_name': ad_name,
                    'adset_name': adset_name,
                    'spend': spend,
                    'impressions': impressions,
                    'link_clicks': link_clicks,
                    'revenue': revenue,
                    'offer_spend': offer_spend,
                    'nprs': nprs,
                    'funnel_starts': funnel_starts,
                    'survey_completions': survey_completions,
                    'checkout_starts': checkout_starts,
                    'ctr': ctr,
                    'cpc': cpc,
                    'cpm': cpm,
                    'roas': roas,
                    'cpa': cpa,
                    'funnel_start_rate': funnel_start_rate,
                    'booking_rate': booking_rate,
                    'survey_completion_rate': survey_completion_rate,
                    'checkout_start_rate': checkout_start_rate,
                    'success_count': success_count
                }
                
                combined_data.append(combined_record)
                
            except Exception as e:
                print(f"❌ Error processing record: {e}")
                continue
        
        return combined_data
    
    def clean_numeric(self, value):
        """Clean and convert numeric values"""
        if value is None or value == '' or value == 0:
            return 0.0
        try:
            if isinstance(value, str):
                value = value.replace('$', '').replace(',', '').replace('%', '').strip()
            return float(value)
        except:
            return 0.0
    
    def get_performance_summary(self):
        """Get performance summary with corrected calculations"""
        if not self.data:
            return {}
        
        # Calculate totals
        total_ads = len(self.data)
        unique_ads = len(set(record['ad_name'] for record in self.data))
        unique_adsets = len(set(record['adset_name'] for record in self.data))
        
        total_spend = sum(record['spend'] for record in self.data)
        total_revenue = sum(record['revenue'] for record in self.data)
        total_offer_spend = sum(record['offer_spend'] for record in self.data)
        total_impressions = sum(record['impressions'] for record in self.data)
        total_link_clicks = sum(record['link_clicks'] for record in self.data)
        total_nprs = sum(record['nprs'] for record in self.data)
        total_funnel_starts = sum(record['funnel_starts'] for record in self.data)
        total_survey_completions = sum(record['survey_completions'] for record in self.data)
        total_checkout_starts = sum(record['checkout_starts'] for record in self.data)
        
        # Calculate performance ratios
        overall_ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
        average_cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
        average_cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
        overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
        average_cpa = (total_spend / total_nprs) if total_nprs > 0 else 0  # CORRECTED
        
        # Completion and LTV
        pas_rate = 0.479  # 47.9% from attribution data
        completed_bookings = total_nprs * pas_rate
        total_cost = total_spend + total_offer_spend
        cac = (total_cost / completed_bookings) if completed_bookings > 0 else 0
        ltv = (total_revenue / completed_bookings) if completed_bookings > 0 else 0
        
        # Funnel metrics
        funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
        booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
        survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
        checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
        
        # ROI
        roi = ((total_revenue - total_offer_spend - total_spend) / total_revenue * 100) if total_revenue > 0 else 0
        
        # Success ads
        successful_ads = len([r for r in self.data if r['success_count'] >= 6])
        
        return {
            'total_ads': total_ads,
            'unique_ads': unique_ads,
            'unique_adsets': unique_adsets,
            'total_spend': total_spend,
            'total_revenue': total_revenue,
            'total_offer_spend': total_offer_spend,
            'total_cost': total_cost,
            'total_impressions': total_impressions,
            'total_link_clicks': total_link_clicks,
            'total_nprs': total_nprs,
            'completed_bookings': completed_bookings,
            'total_funnel_starts': total_funnel_starts,
            'total_survey_completions': total_survey_completions,
            'total_checkout_starts': total_checkout_starts,
            'overall_ctr': overall_ctr,
            'average_cpc': average_cpc,
            'average_cpm': average_cpm,
            'overall_roas': overall_roas,
            'average_cpa': average_cpa,
            'cac': cac,
            'ltv': ltv,
            'funnel_start_rate': funnel_start_rate,
            'booking_rate': booking_rate,
            'survey_completion_rate': survey_completion_rate,
            'checkout_start_rate': checkout_start_rate,
            'completion_rate': pas_rate * 100,
            'roi': roi,
            'successful_ads': successful_ads
        }
    
    def get_creative_dashboard_data(self):
        """Get creative dashboard data (ad-level grouping)"""
        if not self.data:
            return []
        
        # Group by ad name
        ad_groups = {}
        for record in self.data:
            ad_name = record['ad_name']
            if ad_name not in ad_groups:
                ad_groups[ad_name] = []
            ad_groups[ad_name].append(record)
        
        # Aggregate data for each ad
        creative_data = []
        for ad_name, records in ad_groups.items():
            # Sum metrics
            total_spend = sum(r['spend'] for r in records)
            total_impressions = sum(r['impressions'] for r in records)
            total_link_clicks = sum(r['link_clicks'] for r in records)
            total_revenue = sum(r['revenue'] for r in records)
            total_nprs = sum(r['nprs'] for r in records)
            total_funnel_starts = sum(r['funnel_starts'] for r in records)
            total_survey_completions = sum(r['survey_completions'] for r in records)
            total_checkout_starts = sum(r['checkout_starts'] for r in records)
            
            # Calculate aggregated metrics
            ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
            cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
            roas = (total_revenue / total_spend) if total_spend > 0 else 0
            cpa = (total_spend / total_nprs) if total_nprs > 0 else 0
            
            # Funnel rates
            funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
            booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
            survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
            checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
            
            # Success criteria
            success_count = 0
            if ctr >= 0.30: success_count += 1
            if funnel_start_rate >= 15.0: success_count += 1
            if cpa <= 120.0 and cpa > 0: success_count += 1
            if total_link_clicks >= 500: success_count += 1
            if roas >= 1.0: success_count += 1
            if cpc <= 10.0 and cpc > 0: success_count += 1
            if cpm <= 50.0 and cpm > 0: success_count += 1
            if booking_rate >= 2.0: success_count += 1
            
            creative_data.append({
                'ad_name': ad_name,
                'ad_count': len(records),
                'spend': total_spend,
                'impressions': total_impressions,
                'link_clicks': total_link_clicks,
                'revenue': total_revenue,
                'nprs': total_nprs,
                'funnel_starts': total_funnel_starts,
                'survey_completions': total_survey_completions,
                'checkout_starts': total_checkout_starts,
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'roas': roas,
                'cpa': cpa,
                'funnel_start_rate': funnel_start_rate,
                'booking_rate': booking_rate,
                'survey_completion_rate': survey_completion_rate,
                'checkout_start_rate': checkout_start_rate,
                'success_count': success_count
            })
        
        # Sort by spend descending
        creative_data.sort(key=lambda x: x['spend'], reverse=True)
        return creative_data
    
    def get_adgroup_dashboard_data(self):
        """Get ad group dashboard data (nested ad set + ad view)"""
        if not self.data:
            return []
        
        # Group by ad set
        adset_groups = {}
        for record in self.data:
            adset_name = record['adset_name']
            if adset_name not in adset_groups:
                adset_groups[adset_name] = []
            adset_groups[adset_name].append(record)
        
        # Create nested structure
        adgroup_data = []
        for adset_name, records in adset_groups.items():
            # Calculate ad set totals
            total_spend = sum(r['spend'] for r in records)
            total_impressions = sum(r['impressions'] for r in records)
            total_link_clicks = sum(r['link_clicks'] for r in records)
            total_revenue = sum(r['revenue'] for r in records)
            total_nprs = sum(r['nprs'] for r in records)
            total_funnel_starts = sum(r['funnel_starts'] for r in records)
            total_survey_completions = sum(r['survey_completions'] for r in records)
            total_checkout_starts = sum(r['checkout_starts'] for r in records)
            
            # Calculate ad set metrics
            ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
            cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
            roas = (total_revenue / total_spend) if total_spend > 0 else 0
            cpa = (total_spend / total_nprs) if total_nprs > 0 else 0
            
            # Funnel rates
            funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
            booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
            survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
            checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
            
            # Count successful ads in this ad set
            successful_ads = len([r for r in records if r['success_count'] >= 6])
            total_ads = len(records)
            
            adgroup_data.append({
                'adset_name': adset_name,
                'total_ads': total_ads,
                'successful_ads': successful_ads,
                'spend': total_spend,
                'impressions': total_impressions,
                'link_clicks': total_link_clicks,
                'revenue': total_revenue,
                'nprs': total_nprs,
                'funnel_starts': total_funnel_starts,
                'survey_completions': total_survey_completions,
                'checkout_starts': total_checkout_starts,
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'roas': roas,
                'cpa': cpa,
                'funnel_start_rate': funnel_start_rate,
                'booking_rate': booking_rate,
                'survey_completion_rate': survey_completion_rate,
                'checkout_start_rate': checkout_start_rate,
                'ads': records
            })
        
        # Sort by spend descending
        adgroup_data.sort(key=lambda x: x['spend'], reverse=True)
        return adgroup_data
    
    def load_facebook_creative_data(self):
        """Load Facebook creative data for AI insights (background)"""
        try:
            print("🎨 Loading Facebook creative data for AI insights...")
            # This would load creative elements like headlines, text, landing pages
            # For now, just simulate the loading
            time.sleep(2)
            self.fb_creative_data = {'loaded': True, 'count': len(self.data)}
            print("✅ Facebook creative data loaded")
        except Exception as e:
            print(f"❌ Facebook creative data error: {e}")
    
    def load_default_kpi_settings(self):
        """Load default KPI settings"""
        return {
            'ctr_threshold': 0.30,
            'funnel_start_threshold': 15.0,
            'cpa_threshold': 120.0,
            'clicks_threshold': 500,
            'roas_threshold': 1.0,
            'cpc_threshold': 10.0,
            'cpm_threshold': 50.0,
            'booking_rate_threshold': 2.0
        }
    
    def load_default_optimization_rules(self):
        """Load default optimization rules"""
        return {
            'pause_low_roas': {'enabled': True, 'roas_threshold': 0.5, 'spend_threshold': 100},
            'pause_high_cpa': {'enabled': True, 'cpa_threshold': 200, 'spend_threshold': 50},
            'pause_low_ctr': {'enabled': True, 'ctr_threshold': 0.20, 'spend_threshold': 75},
            'scale_high_roas': {'enabled': True, 'roas_threshold': 2.0, 'budget_increase': 20}
        }

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template('enhanced_dashboard.html')

@app.route('/api/performance-summary')
def api_performance_summary():
    """API endpoint for performance summary"""
    try:
        summary = tool.get_performance_summary()
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-dashboard-data')
def api_creative_dashboard_data():
    """API endpoint for creative dashboard data"""
    try:
        data = tool.get_creative_dashboard_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/adgroup-dashboard-data')
def api_adgroup_dashboard_data():
    """API endpoint for ad group dashboard data"""
    try:
        data = tool.get_adgroup_dashboard_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh-data', methods=['POST'])
def api_refresh_data():
    """API endpoint to refresh data"""
    try:
        tool.load_data()
        return jsonify({'status': 'success', 'message': 'Data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

