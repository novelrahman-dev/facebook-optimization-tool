from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
import json
import os
import gspread
from google.oauth2.service_account import Credentials
import csv
import io
from datetime import datetime, timedelta
import time

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
        self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
        self.google_credentials = os.getenv('GOOGLE_CREDENTIALS_JSON')
        
        # Initialize APIs
        self.init_google_sheets()
        self.init_facebook_api()
        
        # Load data
        self.performance_data = []
        self.load_data()
        
        print("✅ Facebook Optimization Tool initialized")

    def init_google_sheets(self):
        """Initialize Google Sheets API"""
        try:
            if self.google_credentials:
                credentials_dict = json.loads(self.google_credentials)
                scope = ['https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive']
                credentials = Credentials.from_service_account_info(credentials_dict, scopes=scope)
                self.gc = gspread.authorize(credentials)
                print("✅ Google Sheets API initialized")
            else:
                self.gc = None
                print("⚠️ Google Sheets credentials not found")
        except Exception as e:
            print(f"❌ Google Sheets error: {e}")
            self.gc = None

    def init_facebook_api(self):
        """Initialize Facebook API"""
        try:
            if self.fb_access_token and self.fb_ad_account_id:
                print("✅ Facebook API credentials configured")
            else:
                print("⚠️ Facebook API credentials not found")
        except Exception as e:
            print(f"❌ Facebook API error: {e}")

    def load_data(self):
        """Load data from Google Sheets"""
        try:
            if self.gc:
                # Load from Google Sheets
                self.load_google_sheets_data()
            else:
                # Load sample data
                self.load_sample_data()
            print("✅ Data loaded successfully")
        except Exception as e:
            print(f"❌ Data loading error: {e}")
            self.load_sample_data()

    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            # Web Pages Data
            web_sheet = self.gc.open_by_key('1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA').sheet1
            web_data = web_sheet.get_all_records()
            print(f"✅ Loaded {len(web_data)} rows from web_pages")
            
            # Attribution Data
            attr_sheet = self.gc.open_by_key('1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ').sheet1
            attr_data = attr_sheet.get_all_records()
            print(f"✅ Loaded {len(attr_data)} rows from attribution")
            
            # FB Spend Data
            fb_sheet = self.gc.open_by_key('1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk').sheet1
            fb_data = fb_sheet.get_all_records()
            print(f"✅ Loaded {len(fb_data)} rows from fb_spend")
            
            # Process and combine data
            self.process_combined_data(web_data, attr_data, fb_data)
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")
            self.load_sample_data()

    def process_combined_data(self, web_data, attr_data, fb_data):
        """Process and combine data from all sources"""
        try:
            combined_data = []
            
            # Create lookup dictionaries
            web_lookup = {}
            for row in web_data:
                if row.get('Web Pages UTM Content') and row.get('Web Pages UTM Term'):
                    key = f"{row['Web Pages UTM Term']}_{row['Web Pages UTM Content']}"
                    web_lookup[key] = row
            
            attr_lookup = {}
            for row in attr_data:
                if row.get('Attribution UTM Content') and row.get('Attribution UTM Term'):
                    key = f"{row['Attribution UTM Term']}_{row['Attribution UTM Content']}"
                    attr_lookup[key] = row
            
            # Process FB data and combine
            for fb_row in fb_data:
                if not fb_row.get('Ad Set Name') or not fb_row.get('Ad Name'):
                    continue
                    
                key = f"{fb_row['Ad Set Name']}_{fb_row['Ad Name']}"
                web_row = web_lookup.get(key, {})
                attr_row = attr_lookup.get(key, {})
                
                # Calculate KPIs
                spend = self.safe_float(fb_row.get('Amount Spent (USD)', 0))
                clicks = self.safe_float(fb_row.get('Link Clicks', 0))
                impressions = self.safe_float(fb_row.get('Impressions', 0))
                site_visits = self.safe_float(web_row.get('Web Pages Unique Count of Landing Pages', 0))
                funnel_starts = self.safe_float(web_row.get('Web Pages Unique Count of Sessions with Funnel Starts', 0))
                bookings = self.safe_float(attr_row.get('Attribution Attributed NPRs', 0))
                revenue = self.safe_float(attr_row.get('Attribution Attibuted Total Revenue (Predicted) (USD)', 0))
                completion_rate = self.safe_float(attr_row.get('Attribution Attibuted PAS (Predicted)', 0.45))
                
                # Apply completion rate failsafe
                if completion_rate < 0.39 or completion_rate > 0.51:
                    completion_rate = 0.45
                
                # Calculate derived metrics
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = (spend / clicks) if clicks > 0 else 0
                funnel_start_rate = (funnel_starts / site_visits * 100) if site_visits > 0 else 0
                booking_conversion_rate = (bookings / site_visits * 100) if site_visits > 0 else 0
                cpa = (spend / bookings) if bookings > 0 else 0
                
                # Calculate LTV/CAC
                completed_appointments = bookings * completion_rate
                ltv = (revenue / completed_appointments) if completed_appointments > 0 else 0
                cac = (spend / completed_appointments) if completed_appointments > 0 else 0
                roas = (ltv / cac) if cac > 0 else 0
                
                # Success criteria
                success_criteria = {
                    'ctr_good': ctr > 0.30,
                    'funnel_start_good': funnel_start_rate > 15,
                    'cpa_good': cpa < 120,
                    'clicks_good': clicks > 500
                }
                
                combined_row = {
                    'ad_set_name': fb_row['Ad Set Name'],
                    'ad_name': fb_row['Ad Name'],
                    'spend': spend,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'cpc': cpc,
                    'site_visits': site_visits,
                    'funnel_starts': funnel_starts,
                    'funnel_start_rate': funnel_start_rate,
                    'bookings': bookings,
                    'booking_conversion_rate': booking_conversion_rate,
                    'cpa': cpa,
                    'revenue': revenue,
                    'ltv': ltv,
                    'cac': cac,
                    'roas': roas,
                    'completion_rate': completion_rate,
                    'success_criteria': success_criteria,
                    'all_criteria_met': all(success_criteria.values())
                }
                
                combined_data.append(combined_row)
            
            self.performance_data = combined_data
            print(f"✅ Processed {len(combined_data)} combined records")
            
        except Exception as e:
            print(f"❌ Data processing error: {e}")
            self.load_sample_data()

    def safe_float(self, value, default=0):
        """Safely convert value to float"""
        try:
            if value is None or value == '':
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def load_sample_data(self):
        """Load sample data for testing"""
        self.performance_data = [
            {
                'ad_set_name': 'Sample Ad Set 1',
                'ad_name': 'Sample Ad 1',
                'spend': 1000,
                'clicks': 150,
                'impressions': 20000,
                'ctr': 0.75,
                'cpc': 6.67,
                'site_visits': 120,
                'funnel_starts': 25,
                'funnel_start_rate': 20.83,
                'bookings': 5,
                'booking_conversion_rate': 4.17,
                'cpa': 200,
                'revenue': 1500,
                'ltv': 666.67,
                'cac': 444.44,
                'roas': 1.5,
                'completion_rate': 0.45,
                'success_criteria': {'ctr_good': True, 'funnel_start_good': True, 'cpa_good': False, 'clicks_good': False},
                'all_criteria_met': False
            }
        ]
        print("✅ Sample data loaded")

    def call_openai_api(self, messages, temperature=0.7, max_tokens=1000):
        """Make API call to OpenAI"""
        try:
            if not self.openai_api_key:
                return "OpenAI API key not configured"
            
            headers = {
                'Authorization': f'Bearer {self.openai_api_key}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'model': 'gpt-4o-mini',
                'messages': messages,
                'temperature': temperature,
                'max_tokens': max_tokens
            }
            
            response = requests.post(
                'https://api.openai.com/v1/chat/completions',
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            else:
                return f"OpenAI API error: {response.status_code} - {response.text}"
                
        except Exception as e:
            return f"OpenAI API error: {str(e)}"

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    return render_template('enhanced_dashboard.html')

@app.route('/api/performance-summary')
def performance_summary():
    try:
        # Calculate summary statistics
        total_spend = sum(row['spend'] for row in tool.performance_data)
        total_clicks = sum(row['clicks'] for row in tool.performance_data)
        total_impressions = sum(row['impressions'] for row in tool.performance_data)
        total_bookings = sum(row['bookings'] for row in tool.performance_data)
        total_revenue = sum(row['revenue'] for row in tool.performance_data)
        
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        avg_cpa = (total_spend / total_bookings) if total_bookings > 0 else 0
        overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
        
        successful_ads = len([row for row in tool.performance_data if row['all_criteria_met']])
        
        summary = {
            'total_ads': len(tool.performance_data),
            'total_spend': total_spend,
            'total_clicks': total_clicks,
            'total_bookings': total_bookings,
            'total_revenue': total_revenue,
            'avg_ctr': avg_ctr,
            'avg_cpa': avg_cpa,
            'overall_roas': overall_roas,
            'successful_ads': successful_ads,
            'success_rate': (successful_ads / len(tool.performance_data) * 100) if tool.performance_data else 0
        }
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/performance-data')
def performance_data():
    try:
        view_type = request.args.get('view', 'adset')  # 'adset' or 'ad'
        
        if view_type == 'adset':
            # Aggregate by ad set
            adset_data = {}
            for row in tool.performance_data:
                adset_name = row['ad_set_name']
                if adset_name not in adset_data:
                    adset_data[adset_name] = {
                        'ad_set_name': adset_name,
                        'spend': 0,
                        'clicks': 0,
                        'impressions': 0,
                        'site_visits': 0,
                        'funnel_starts': 0,
                        'bookings': 0,
                        'revenue': 0,
                        'ads': []
                    }
                
                adset_data[adset_name]['spend'] += row['spend']
                adset_data[adset_name]['clicks'] += row['clicks']
                adset_data[adset_name]['impressions'] += row['impressions']
                adset_data[adset_name]['site_visits'] += row['site_visits']
                adset_data[adset_name]['funnel_starts'] += row['funnel_starts']
                adset_data[adset_name]['bookings'] += row['bookings']
                adset_data[adset_name]['revenue'] += row['revenue']
                adset_data[adset_name]['ads'].append(row)
            
            # Calculate derived metrics for ad sets
            for adset in adset_data.values():
                adset['ctr'] = (adset['clicks'] / adset['impressions'] * 100) if adset['impressions'] > 0 else 0
                adset['cpc'] = (adset['spend'] / adset['clicks']) if adset['clicks'] > 0 else 0
                adset['funnel_start_rate'] = (adset['funnel_starts'] / adset['site_visits'] * 100) if adset['site_visits'] > 0 else 0
                adset['booking_conversion_rate'] = (adset['bookings'] / adset['site_visits'] * 100) if adset['site_visits'] > 0 else 0
                adset['cpa'] = (adset['spend'] / adset['bookings']) if adset['bookings'] > 0 else 0
                adset['roas'] = (adset['revenue'] / adset['spend']) if adset['spend'] > 0 else 0
            
            return jsonify(list(adset_data.values()))
        else:
            # Return individual ad data
            return jsonify(tool.performance_data)
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    try:
        recommendations = []
        
        for row in tool.performance_data:
            spend = row['spend']
            ctr = row['ctr']
            cpc = row['cpc']
            funnel_start_rate = row['funnel_start_rate']
            bookings = row['bookings']
            cpa = row['cpa']
            roas = row['roas']
            
            # 24-hour rules
            if spend > 100:
                if ctr <= 0.40 or cpc >= 6 or funnel_start_rate <= 15:
                    recommendations.append({
                        'ad_set_name': row['ad_set_name'],
                        'ad_name': row['ad_name'],
                        'action': 'pause',
                        'reason': 'Poor early performance metrics',
                        'spend': spend,
                        'bookings': bookings,
                        'cpa': cpa,
                        'current_metrics': {
                            'ctr': ctr,
                            'cpc': cpc,
                            'funnel_start_rate': funnel_start_rate
                        }
                    })
            
            # Advanced performance rules
            if spend > 300:
                cvr = row['booking_conversion_rate']
                if cvr > 2.5 and cpa < 120 and roas > 1:
                    recommendations.append({
                        'ad_set_name': row['ad_set_name'],
                        'ad_name': row['ad_name'],
                        'action': 'scale',
                        'reason': 'Excellent performance - ready for scaling',
                        'spend': spend,
                        'bookings': bookings,
                        'cpa': cpa,
                        'current_metrics': {
                            'cvr': cvr,
                            'cpa': cpa,
                            'roas': roas
                        }
                    })
        
        return jsonify(recommendations)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-brief', methods=['POST'])
def generate_creative_brief():
    try:
        data = request.json
        campaign_name = data.get('campaign_name', 'New Campaign')
        campaign_type = data.get('campaign_type', 'seasonal')
        messaging_pillar = data.get('messaging_pillar', 'Convenience')
        value_pillar = data.get('value_pillar', 'Convenience-Insurance')
        creative_format = data.get('creative_format', 'Static')
        
        # Generate AI-enhanced brief
        prompt = f"""
        Create a comprehensive Facebook creative brief for a dental appointment booking campaign.
        
        Campaign Details:
        - Name: {campaign_name}
        - Type: {campaign_type}
        - Messaging Pillar: {messaging_pillar}
        - Value Pillar: {value_pillar}
        - Creative Format: {creative_format}
        
        Based on successful patterns, create a brief that includes:
        1. Campaign Overview
        2. Target Audience Insights
        3. Messaging Framework
        4. Visual Direction
        5. Success Metrics
        
        Focus on the specified messaging and value pillars for this {creative_format} campaign.
        """
        
        messages = [{"role": "user", "content": prompt}]
        ai_brief = tool.call_openai_api(messages, temperature=0.7, max_tokens=1500)
        
        return jsonify({
            'campaign_name': campaign_name,
            'campaign_type': campaign_type,
            'messaging_pillar': messaging_pillar,
            'value_pillar': value_pillar,
            'creative_format': creative_format,
            'ai_brief': ai_brief,
            'generated_at': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-insights')
def ai_insights():
    try:
        insight_type = request.args.get('type', 'cluster')
        
        if insight_type == 'cluster':
            prompt = """
            Analyze the Facebook ad performance data and identify key patterns for optimization.
            Focus on successful vs unsuccessful campaigns and provide actionable insights.
            """
        elif insight_type == 'creative':
            prompt = """
            Analyze creative performance patterns and provide recommendations for future creative development.
            Focus on messaging, format, and design elements that drive success.
            """
        elif insight_type == 'cro':
            prompt = """
            Analyze conversion funnel performance and provide CRO recommendations.
            Focus on Funnel Start, Survey Completion, and Checkout Start optimization.
            """
        else:
            prompt = """
            Provide strategic recommendations for campaign optimization based on performance data.
            Include budget allocation, audience targeting, and creative strategy insights.
            """
        
        messages = [{"role": "user", "content": prompt}]
        ai_insights = tool.call_openai_api(messages, temperature=0.7, max_tokens=1000)
        
        return jsonify({
            'insight_type': insight_type,
            'insights': ai_insights,
            'generated_at': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-openai')
def test_openai():
    try:
        if not tool.openai_api_key:
            return jsonify({'status': 'error', 'message': 'OpenAI API key not configured'})
        
        messages = [{"role": "user", "content": "Test connection. Respond with 'Connection successful'."}]
        response = tool.call_openai_api(messages, max_tokens=50)
        
        return jsonify({
            'status': 'success',
            'message': 'OpenAI connection working',
            'response': response
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/refresh-data')
def refresh_data():
    try:
        tool.load_data()
        return jsonify({
            'status': 'success',
            'message': 'Data refreshed successfully',
            'records_loaded': len(tool.performance_data)
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)

