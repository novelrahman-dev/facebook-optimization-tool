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
import io
import base64

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.openai_client = None  # Initialize as None first
        self.gc = None
        self.setup_apis()
        self.load_data()
        self.setup_automation()
        
    def setup_apis(self):
        """Initialize API clients"""
        try:
            # OpenAI API (simplified initialization)
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key and api_key.startswith('sk-'):
                try:
                    from openai import OpenAI
                    # Simple initialization without extra parameters
                    self.openai_client = OpenAI(api_key=api_key)
                    print("✅ OpenAI API client initialized")
                except ImportError:
                    print("❌ OpenAI package not found")
                    self.openai_client = None
                except Exception as e:
                    print(f"❌ OpenAI client error: {e}")
                    # Try alternative initialization
                    try:
                        self.openai_client = OpenAI()  # Use environment variable directly
                        print("✅ OpenAI API client initialized (alternative method)")
                    except:
                        self.openai_client = None
            else:
                self.openai_client = None
                print("⚠️ OpenAI API key not found or invalid format")
            
            # Google Sheets API
            self.setup_google_sheets()
            
            # Facebook API (placeholder for now)
            self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
            self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
            
        except Exception as e:
            print(f"❌ API setup error: {e}")
    
    def setup_google_sheets(self):
        """Setup Google Sheets API connection"""
        try:
            # Use service account credentials
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            # For Railway deployment, we'll use environment variable for credentials
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
    
    def load_data(self):
        """Load and process data from all sources"""
        try:
            # Load sample data for demo (replace with real data loading)
            self.performance_data = self.load_sample_data()
            self.last_refresh = datetime.now()
            print("✅ Data loaded successfully")
            
        except Exception as e:
            print(f"❌ Data loading error: {e}")
            self.performance_data = pd.DataFrame()
            self.last_refresh = None
    
    def load_sample_data(self):
        """Load sample data for demonstration"""
        # Create sample performance data
        np.random.seed(42)  # For consistent sample data
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
        
        # Calculate KPIs
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
            # Google Sheets IDs
            sheet_ids = {
                'web_pages': '1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA',
                'attribution': '1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ',
                'fb_spend': '1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk'
            }
            
            # Load each sheet
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
            if self.openai_client:
                # Simple test call
                response = self.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": "Hello, this is a test."}],
                    max_tokens=10
                )
                return True
            return False
        except Exception as e:
            print(f"OpenAI test failed: {e}")
            return False
    
    def generate_ai_insights(self, insight_type='cluster'):
        """Generate AI-powered insights using OpenAI"""
        try:
            # Check if OpenAI client is available and working
            if not self.openai_client:
                return "OpenAI API not configured. Please add your OPENAI_API_KEY environment variable and restart the application."
            
            # Test connection first
            if not self.test_openai_connection():
                return "OpenAI API connection failed. Please check your API key and try again."
            
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
            
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"Error generating AI insights: {e}"
    
    def generate_creative_brief(self, campaign_name, campaign_type):
        """Generate AI-enhanced creative brief"""
        try:
            # Check if OpenAI client is available
            if not self.openai_client:
                return {
                    'campaign_name': campaign_name,
                    'campaign_type': campaign_type,
                    'generated_at': datetime.now().isoformat(),
                    'error': 'OpenAI API not configured',
                    'ai_content': 'Please add your OPENAI_API_KEY environment variable and restart the application to enable AI features.'
                }
            
            # Test connection first
            if not self.test_openai_connection():
                return {
                    'campaign_name': campaign_name,
                    'campaign_type': campaign_type,
                    'generated_at': datetime.now().isoformat(),
                    'error': 'OpenAI API connection failed',
                    'ai_content': 'OpenAI API connection failed. Please check your API key and try again.'
                }
            
            # Get performance insights
            top_performers = self.performance_data.nlargest(5, 'roas')
            
            prompt = f"""
            Generate a comprehensive creative brief for a Facebook campaign with these details:
            
            Campaign Name: {campaign_name}
            Campaign Type: {campaign_type}
            
            Based on recent performance data:
            - Top performing ads have ROAS between {top_performers['roas'].min():.2f} and {top_performers['roas'].max():.2f}
            - Best CTR achieved: {top_performers['ctr'].max():.2f}%
            - Most efficient CPC: ${top_performers['cpc'].min():.2f}
            
            Include:
            1. Campaign Overview (objectives, KPIs, placements)
            2. Target Audience Insights
            3. Messaging Framework (3 headline variations, 2 primary text options)
            4. Visual Direction
            5. Success Metrics and Testing Plan
            
            Make it actionable and specific to Facebook advertising.
            """
            
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
                temperature=0.7
            )
            
            return {
                'campaign_name': campaign_name,
                'campaign_type': campaign_type,
                'generated_at': datetime.now().isoformat(),
                'data_freshness': self.last_refresh.isoformat() if self.last_refresh else None,
                'ai_content': response.choices[0].message.content,
                'performance_context': {
                    'top_roas': float(top_performers['roas'].max()),
                    'best_ctr': float(top_performers['ctr'].max()),
                    'best_cpc': float(top_performers['cpc'].min())
                }
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
                    time.sleep(60)  # Check every minute
            
            scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            scheduler_thread.start()
            
            print("✅ Automated scheduler started")
            
        except Exception as e:
            print(f"❌ Automation setup error: {e}")
    
    def refresh_all_data(self):
        """Refresh all data sources"""
        print("🔄 Starting automated data refresh...")
        success = self.refresh_google_sheets_data()
        if success:
            self.load_data()
            print("✅ Automated data refresh completed")
        else:
            print("❌ Automated data refresh failed")
    
    def run_daily_analysis(self):
        """Run daily AI analysis"""
        print("🤖 Running daily AI analysis...")
        try:
            # Generate insights and store them
            cluster_insights = self.generate_ai_insights('cluster')
            creative_insights = self.generate_ai_insights('creative_brief')
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
            'last_refresh': tool.last_refresh.isoformat() if tool.last_refresh else None
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

@app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    """Manually refresh data"""
    try:
        success = tool.refresh_google_sheets_data()
        if success:
            tool.load_data()
            return jsonify({
                'status': 'success',
                'message': 'Data refreshed successfully',
                'last_refresh': tool.last_refresh.isoformat()
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to refresh data'
            }), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/automation-status')
def automation_status():
    """Get automation status"""
    return jsonify({
        'auto_refresh_enabled': True,
        'last_refresh': tool.last_refresh.isoformat() if tool.last_refresh else None,
        'next_scheduled_refresh': 'Every 12 hours',
        'next_scheduled_analysis': 'Daily at 9:00 AM'
    })

@app.route('/api/toggle-automation', methods=['POST'])
def toggle_automation():
    """Toggle automation on/off"""
    # For now, just return current status
    return jsonify({
        'auto_refresh_enabled': True,
        'message': 'Automation is always enabled in this version'
    })

@app.route('/api/test-openai')
def test_openai():
    """Test OpenAI connection"""
    try:
        if tool.openai_client:
            success = tool.test_openai_connection()
            return jsonify({
                'status': 'success' if success else 'failed',
                'message': 'OpenAI connection working' if success else 'OpenAI connection failed'
            })
        else:
            return jsonify({
                'status': 'failed',
                'message': 'OpenAI client not initialized'
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Get port from environment variable (Railway sets this)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
