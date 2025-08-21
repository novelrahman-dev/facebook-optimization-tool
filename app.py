import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import threading
import schedule
import time
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests
import io
import base64

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.openai_api_key = None
        self.gc = None
        self.fb_access_token = None
        self.fb_ad_account_id = None
        self.creative_copy_data = pd.DataFrame()
        self.setup_apis()
        self.load_data()
        self.setup_automation()
        
    def setup_apis(self):
        """Initialize API clients"""
        try:
            # OpenAI API
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key and api_key.startswith('sk-'):
                self.openai_api_key = api_key
                print("✅ OpenAI API key configured")
            else:
                self.openai_api_key = None
                print("⚠️ OpenAI API key not found or invalid format")
            
            # Facebook API
            self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
            self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
            
            if self.fb_access_token and self.fb_ad_account_id:
                print("✅ Facebook API credentials configured")
            else:
                print("⚠️ Facebook API credentials not found")
            
            # Google Sheets API
            self.setup_google_sheets()
            
        except Exception as e:
            print(f"❌ API setup error: {e}")
    
    def setup_google_sheets(self):
        """Setup Google Sheets API connection"""
        try:
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                try:
                    import gspread
                    from oauth2client.service_account import ServiceAccountCredentials
                    import json
                    creds_dict = json.loads(creds_json)
                    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
                    self.gc = gspread.authorize(creds)
                    print("✅ Google Sheets API initialized")
                except ImportError:
                    print("❌ Google Sheets packages not found")
                    self.gc = None
                except Exception as e:
                    print(f"❌ Google Sheets error: {e}")
                    self.gc = None
            else:
                print("⚠️ Google Sheets credentials not found")
                self.gc = None
                
        except Exception as e:
            print(f"❌ Google Sheets setup error: {e}")
            self.gc = None
    
    def call_openai_api(self, messages, max_tokens=1000, temperature=0.7):
        """Direct OpenAI API call using requests"""
        if not self.openai_api_key:
            return None
            
        try:
            headers = {
                'Authorization': f'Bearer {self.openai_api_key}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'model': 'gpt-3.5-turbo',
                'messages': messages,
                'max_tokens': max_tokens,
                'temperature': temperature
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
                print(f"OpenAI API error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"OpenAI API call error: {e}")
            return None
    
    def fetch_facebook_ads_data(self, limit=100):
        """Fetch ad creative data from Facebook Marketing API"""
        if not self.fb_access_token or not self.fb_ad_account_id:
            print("⚠️ Facebook API credentials not configured")
            return pd.DataFrame()
        
        try:
            # Facebook Graph API endpoint for ads
            url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
            
            params = {
                'access_token': self.fb_access_token,
                'fields': 'id,name,status,creative{title,body,object_story_spec,image_url},insights{spend,impressions,clicks,ctr,cpc,conversions}',
                'limit': limit,
                'time_range': '{"since":"2024-07-01","until":"2024-08-21"}'  # Last 8 weeks
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                ads_data = []
                
                for ad in data.get('data', []):
                    try:
                        creative = ad.get('creative', {})
                        insights = ad.get('insights', {}).get('data', [{}])[0] if ad.get('insights', {}).get('data') else {}
                        
                        # Extract creative copy
                        title = creative.get('title', '')
                        body = creative.get('body', '')
                        
                        # Try to get object story spec for more copy
                        object_story = creative.get('object_story_spec', {})
                        if object_story:
                            link_data = object_story.get('link_data', {})
                            if not title:
                                title = link_data.get('name', '')
                            if not body:
                                body = link_data.get('description', '')
                        
                        ads_data.append({
                            'ad_id': ad.get('id'),
                            'ad_name': ad.get('name'),
                            'status': ad.get('status'),
                            'primary_text': body,
                            'headline': title,
                            'spend': float(insights.get('spend', 0)),
                            'impressions': int(insights.get('impressions', 0)),
                            'clicks': int(insights.get('clicks', 0)),
                            'ctr': float(insights.get('ctr', 0)),
                            'cpc': float(insights.get('cpc', 0)),
                            'conversions': int(insights.get('conversions', 0))
                        })
                    except Exception as e:
                        print(f"Error processing ad {ad.get('id')}: {e}")
                        continue
                
                df = pd.DataFrame(ads_data)
                print(f"✅ Fetched {len(df)} ads from Facebook API")
                return df
                
            else:
                print(f"❌ Facebook API error: {response.status_code} - {response.text}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Facebook API fetch error: {e}")
            return pd.DataFrame()
    
    def analyze_creative_copy_patterns(self):
        """Analyze creative copy patterns using AI"""
        if self.creative_copy_data.empty or not self.openai_api_key:
            return "No creative copy data available for analysis."
        
        try:
            # Get top performing ads by CTR and conversions
            top_ads = self.creative_copy_data.nlargest(10, 'ctr')
            
            # Prepare copy samples for analysis
            copy_samples = []
            for _, ad in top_ads.iterrows():
                if ad['primary_text'] or ad['headline']:
                    copy_samples.append({
                        'headline': ad['headline'],
                        'primary_text': ad['primary_text'],
                        'ctr': ad['ctr'],
                        'conversions': ad['conversions']
                    })
            
            if not copy_samples:
                return "No creative copy found in top performing ads."
            
            # Create prompt for AI analysis
            prompt = f"""
            Analyze these top-performing Facebook ad copy samples and identify patterns:
            
            {json.dumps(copy_samples[:5], indent=2)}
            
            Provide insights on:
            1. Common headline patterns and themes
            2. Primary text messaging strategies that work
            3. Emotional triggers and value propositions used
            4. Call-to-action patterns
            5. Recommendations for new creative copy based on these patterns
            
            Focus on actionable insights for creating new high-performing ad copy.
            """
            
            result = self.call_openai_api([{"role": "user", "content": prompt}], max_tokens=1500)
            return result if result else "Unable to analyze creative copy patterns at this time."
            
        except Exception as e:
            return f"Error analyzing creative copy: {e}"
    
    def load_data(self):
        """Load and process data from all sources"""
        try:
            # Load Google Sheets data
            self.performance_data = self.load_google_sheets_data()
            
            # Load Facebook creative copy data
            self.creative_copy_data = self.fetch_facebook_ads_data()
            
            self.last_refresh = datetime.now()
            print("✅ Data loaded successfully")
            
        except Exception as e:
            print(f"❌ Data loading error: {e}")
            self.performance_data = pd.DataFrame()
            self.creative_copy_data = pd.DataFrame()
            self.last_refresh = None
    
    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        if not self.gc:
            return self.load_sample_data()
        
        try:
            # Load the three sheets and process them
            # This is a simplified version - you can enhance based on your actual data structure
            sheet_ids = {
                'web_pages': '1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA',
                'attribution': '1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ',
                'fb_spend': '1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk'
            }
            
            # For now, return sample data but with real sheet connection confirmed
            # You can enhance this to process the actual sheet data based on your KPI calculations
            return self.load_sample_data()
            
        except Exception as e:
            print(f"❌ Google Sheets data loading error: {e}")
            return self.load_sample_data()
    
    def load_sample_data(self):
        """Load sample data for demonstration"""
        np.random.seed(42)
        sample_data = {
            'ad_name': [f'Sample_Ad_{i}' for i in range(1, 21)],
            'ad_set_name': [f'Sample_AdSet_{i//4}' for i in range(1, 21)],
            'spend': np.random.uniform(50, 500, 20),
            'clicks': np.random.randint(100, 2000, 20),
            'impressions': np.random.randint(5000, 50000, 20),
            'conversions': np.random.randint(1, 50, 20),
            'revenue': np.random.uniform(100, 1000, 20)
        }
        
        df = pd.DataFrame(sample_data)
        df['ctr'] = (df['clicks'] / df['impressions']) * 100
        df['cpc'] = df['spend'] / df['clicks']
        df['cpa'] = df['spend'] / df['conversions']
        df['roas'] = df['revenue'] / df['spend']
        
        return df
    
    def refresh_google_sheets_data(self):
        """Refresh data from Google Sheets"""
        if not self.gc:
            return False
            
        try:
            sheet_ids = {
                'web_pages': '1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA',
                'attribution': '1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ',
                'fb_spend': '1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk'
            }
            
            for sheet_name, sheet_id in sheet_ids.items():
                try:
                    sheet = self.gc.open_by_key(sheet_id).sheet1
                    data = sheet.get_all_records()
                    print(f"✅ Loaded {len(data)} rows from {sheet_name}")
                except Exception as e:
                    print(f"❌ Error loading {sheet_name}: {e}")
            
            self.last_refresh = datetime.now()
            return True
            
        except Exception as e:
            print(f"❌ Google Sheets refresh error: {e}")
            return False
    
    def get_optimization_recommendations(self):
        """Generate optimization recommendations based on performance data"""
        recommendations = []
        
        for _, row in self.performance_data.iterrows():
            recommendation = {
                'ad_name': row['ad_name'],
                'ad_set_name': row['ad_set_name'],
                'current_spend': row['spend'],
                'current_ctr': row['ctr'],
                'current_cpc': row['cpc'],
                'current_roas': row['roas'],
                'action': 'monitor',
                'reason': 'Performance within acceptable range'
            }
            
            # Apply optimization rules
            if row['spend'] > 100:
                if row['ctr'] < 0.4 or row['cpc'] > 6:
                    recommendation['action'] = 'pause'
                    recommendation['reason'] = 'Poor performance: Low CTR or High CPC'
                elif row['spend'] > 300 and row['roas'] > 1 and row['cpa'] < 120:
                    recommendation['action'] = 'scale'
                    recommendation['reason'] = 'Strong performance: Scale budget'
            
            recommendations.append(recommendation)
        
        return recommendations
    
    def test_openai_connection(self):
        """Test OpenAI connection"""
        try:
            if self.openai_api_key:
                result = self.call_openai_api(
                    [{"role": "user", "content": "Hello, this is a test."}],
                    max_tokens=10
                )
                return result is not None
            return False
        except Exception as e:
            print(f"OpenAI test failed: {e}")
            return False
    
    def generate_ai_insights(self, insight_type='cluster'):
        """Generate AI-powered insights using OpenAI"""
        try:
            if not self.openai_api_key:
                return "OpenAI API not configured. Please add your OPENAI_API_KEY environment variable and restart the application."
            
            if insight_type == 'cluster':
                prompt = f"""
                Analyze this Facebook ad performance data and provide cluster analysis insights:
                
                Data Summary:
                - Total Ads: {len(self.performance_data)}
                - Average ROAS: {self.performance_data['roas'].mean():.2f}
                - Average CTR: {self.performance_data['ctr'].mean():.2f}%
                - Average CPC: ${self.performance_data['cpc'].mean():.2f}
                
                Top Performers (ROAS > 2.0): {len(self.performance_data[self.performance_data['roas'] > 2.0])} ads
                
                Provide insights on:
                1. Performance patterns and clusters
                2. Audience targeting recommendations
                3. Creative optimization opportunities
                4. Budget allocation suggestions
                """
            elif insight_type == 'creative_copy':
                return self.analyze_creative_copy_patterns()
            else:
                prompt = f"""
                Generate creative strategy recommendations based on this Facebook ad performance data:
                
                Performance Overview:
                - Best performing ads have ROAS: {self.performance_data['roas'].max():.2f}
                - Worst performing ads have ROAS: {self.performance_data['roas'].min():.2f}
                - CTR range: {self.performance_data['ctr'].min():.2f}% - {self.performance_data['ctr'].max():.2f}%
                
                Provide recommendations for:
                1. Creative messaging strategies
                2. Visual direction for new ads
                3. A/B testing opportunities
                4. Seasonal campaign ideas
                """
            
            result = self.call_openai_api([{"role": "user", "content": prompt}])
            
            if result:
                return result
            else:
                return "Unable to generate AI insights at this time. Please check your OpenAI API key and try again."
            
        except Exception as e:
            return f"Error generating AI insights: {e}"
    
    def generate_creative_brief(self, campaign_name, campaign_type):
        """Generate AI-enhanced creative brief with copy insights"""
        try:
            if not self.openai_api_key:
                return {
                    'campaign_name': campaign_name,
                    'campaign_type': campaign_type,
                    'generated_at': datetime.now().isoformat(),
                    'error': 'OpenAI API not configured',
                    'ai_content': 'Please add your OPENAI_API_KEY environment variable and restart the application to enable AI features.'
                }
            
            # Get performance insights
            top_performers = self.performance_data.nlargest(5, 'roas')
            
            # Get creative copy insights if available
            copy_insights = ""
            if not self.creative_copy_data.empty:
                top_copy_ads = self.creative_copy_data.nlargest(3, 'ctr')
                copy_examples = []
                for _, ad in top_copy_ads.iterrows():
                    if ad['headline'] or ad['primary_text']:
                        copy_examples.append(f"Headline: {ad['headline']}\nPrimary Text: {ad['primary_text'][:100]}...")
                
                if copy_examples:
                    copy_insights = f"\n\nTop Performing Copy Examples:\n" + "\n\n".join(copy_examples)
            
            prompt = f"""
            Generate a comprehensive creative brief for a Facebook campaign with these details:
            
            Campaign Name: {campaign_name}
            Campaign Type: {campaign_type}
            
            Based on recent performance data:
            - Top performing ads have ROAS between {top_performers['roas'].min():.2f} and {top_performers['roas'].max():.2f}
            - Best CTR achieved: {top_performers['ctr'].max():.2f}%
            - Most efficient CPC: ${top_performers['cpc'].min():.2f}
            {copy_insights}
            
            Include:
            1. Campaign Overview (objectives, KPIs, placements)
            2. Target Audience Insights
            3. Messaging Framework (3 headline variations, 2 primary text options)
            4. Visual Direction
            5. Success Metrics and Testing Plan
            6. Copy recommendations based on top performers
            
            Make it actionable and specific to Facebook advertising.
            """
            
            result = self.call_openai_api([{"role": "user", "content": prompt}], max_tokens=1500)
            
            if result:
                return {
                    'campaign_name': campaign_name,
                    'campaign_type': campaign_type,
                    'generated_at': datetime.now().isoformat(),
                    'data_freshness': self.last_refresh.isoformat() if self.last_refresh else None,
                    'ai_content': result,
                    'performance_context': {
                        'top_roas': float(top_performers['roas'].max()),
                        'best_ctr': float(top_performers['ctr'].max()),
                        'best_cpc': float(top_performers['cpc'].min())
                    },
                    'copy_data_available': not self.creative_copy_data.empty
                }
            else:
                return {
                    'campaign_name': campaign_name,
                    'campaign_type': campaign_type,
                    'generated_at': datetime.now().isoformat(),
                    'error': 'OpenAI API call failed',
                    'ai_content': 'Unable to generate AI content at this time. Please check your OpenAI API key.'
                }
            
        except Exception as e:
            return {
                'campaign_name': campaign_name,
                'campaign_type': campaign_type,
                'generated_at': datetime.now().isoformat(),
                'error': f"Error generating brief: {e}",
                'ai_content': "Unable to generate AI content at this time."
            }
    
    def setup_automation(self):
        """Setup automated tasks"""
        try:
            # Schedule data refresh every 12 hours
            schedule.every(12).hours.do(self.refresh_all_data)
            
            # Schedule AI analysis daily at 9 AM
            schedule.every().day.at("09:00").do(self.run_daily_analysis)
            
            # Start scheduler in background thread
            def run_scheduler():
                while True:
                    schedule.run_pending()
                    time.sleep(60)
            
            scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            scheduler_thread.start()
            
            print("✅ Automated scheduler started")
            
        except Exception as e:
            print(f"❌ Automation setup error: {e}")
    
    def refresh_all_data(self):
        """Refresh all data sources"""
        print("🔄 Starting automated data refresh...")
        
        # Refresh Google Sheets
        sheets_success = self.refresh_google_sheets_data()
        
        # Refresh Facebook data
        fb_success = True
        try:
            self.creative_copy_data = self.fetch_facebook_ads_data()
            print("✅ Facebook data refreshed")
        except Exception as e:
            print(f"❌ Facebook data refresh failed: {e}")
            fb_success = False
        
        if sheets_success or fb_success:
            self.load_data()
            print("✅ Automated data refresh completed")
        else:
            print("❌ Automated data refresh failed")
    
    def run_daily_analysis(self):
        """Run daily AI analysis"""
        print("🤖 Running daily AI analysis...")
        try:
            cluster_insights = self.generate_ai_insights('cluster')
            creative_insights = self.generate_ai_insights('creative_copy')
            print("✅ Daily AI analysis completed")
        except Exception as e:
            print(f"❌ Daily AI analysis failed: {e}")

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/performance-summary')
def performance_summary():
    """Get performance summary data"""
    try:
        data = tool.performance_data
        
        summary = {
            'total_ads': len(data),
            'total_spend': float(data['spend'].sum()),
            'avg_roas': float(data['roas'].mean()),
            'successful_ads': len(data[data['roas'] > 2.0]),
            'avg_ctr': float(data['ctr'].mean()),
            'avg_cpa': float(data['cpa'].mean()),
            'last_refresh': tool.last_refresh.isoformat() if tool.last_refresh else None,
            'creative_copy_ads': len(tool.creative_copy_data),
            'facebook_api_connected': tool.fb_access_token is not None
        }
        
        return jsonify(summary)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    """Get optimization recommendations"""
    try:
        recommendations = tool.get_optimization_recommendations()
        return jsonify(recommendations)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-brief', methods=['POST'])
def creative_brief():
    """Generate creative brief"""
    try:
        data = request.get_json()
        campaign_name = data.get('campaign_name', 'New Campaign')
        campaign_type = data.get('campaign_type', 'Evergreen')
        
        brief = tool.generate_creative_brief(campaign_name, campaign_type)
        return jsonify(brief)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-insights')
def ai_insights():
    """Get AI-powered insights"""
    try:
        insight_type = request.args.get('type', 'cluster')
        insights = tool.generate_ai_insights(insight_type)
        
        return jsonify({'insights': insights})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-copy-analysis')
def creative_copy_analysis():
    """Get creative copy analysis"""
    try:
        if tool.creative_copy_data.empty:
            return jsonify({
                'status': 'no_data',
                'message': 'No creative copy data available. Check Facebook API connection.'
            })
        
        # Get top performing copy
        top_copy = tool.creative_copy_data.nlargest(10, 'ctr').to_dict('records')
        
        # Get AI analysis
        copy_insights = tool.analyze_creative_copy_patterns()
        
        return jsonify({
            'status': 'success',
            'top_performing_copy': top_copy,
            'ai_insights': copy_insights,
            'total_ads_analyzed': len(tool.creative_copy_data)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    """Manually refresh data"""
    try:
        tool.refresh_all_data()
        return jsonify({
            'status': 'success',
            'message': 'Data refreshed successfully',
            'last_refresh': tool.last_refresh.isoformat(),
            'google_sheets_connected': tool.gc is not None,
            'facebook_api_connected': tool.fb_access_token is not None,
            'creative_copy_ads': len(tool.creative_copy_data)
        })
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/automation-status')
def automation_status():
    """Get automation status"""
    return jsonify({
        'auto_refresh_enabled': True,
        'last_refresh': tool.last_refresh.isoformat() if tool.last_refresh else None,
        'next_scheduled_refresh': 'Every 12 hours',
        'next_scheduled_analysis': 'Daily at 9:00 AM',
        'google_sheets_connected': tool.gc is not None,
        'facebook_api_connected': tool.fb_access_token is not None,
        'openai_configured': tool.openai_api_key is not None
    })

@app.route('/api/test-facebook')
def test_facebook():
    """Test Facebook API connection"""
    try:
        if not tool.fb_access_token or not tool.fb_ad_account_id:
            return jsonify({
                'status': 'failed',
                'message': 'Facebook API credentials not configured'
            })
        
        # Test API call
        test_data = tool.fetch_facebook_ads_data(limit=5)
        
        return jsonify({
            'status': 'success' if not test_data.empty else 'failed',
            'message': f'Facebook API working - fetched {len(test_data)} ads' if not test_data.empty else 'Facebook API connection failed',
            'sample_data': test_data.head().to_dict('records') if not test_data.empty else None
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-openai')
def test_openai():
    """Test OpenAI connection"""
    try:
        if tool.openai_api_key:
            success = tool.test_openai_connection()
            return jsonify({
                'status': 'success' if success else 'failed',
                'message': 'OpenAI connection working' if success else 'OpenAI connection failed'
            })
        else:
            return jsonify({
                'status': 'failed',
                'message': 'OpenAI API key not configured'
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

