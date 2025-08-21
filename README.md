# 🚀 Facebook Media Buying Optimization Tool

AI-powered Facebook advertising optimization tool with automated performance analysis, creative brief generation, and real-time insights.

## ✨ Features

- **📊 Performance Dashboard** - Real-time KPI tracking and analysis
- **🚀 Optimization Engine** - AI-powered recommendations for scaling/pausing ads
- **🎯 Creative Brief Generator** - AI-enhanced briefs based on winning patterns
- **🤖 AI Insights** - Deep cluster analysis and creative strategy recommendations
- **🔄 Automated Data Refresh** - Continuous updates from Google Sheets and Facebook API

## 🛠️ Quick Deploy to Railway

### 1. Deploy Button (Easiest)
[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new/template)

### 2. Manual Deployment

1. **Fork this repository**
2. **Connect to Railway**:
   - Go to [Railway.app](https://railway.app)
   - Sign up with GitHub
   - Create new project from GitHub repo

3. **Set Environment Variables** in Railway dashboard:
   ```
   OPENAI_API_KEY=sk-your_key_here
   GOOGLE_CREDENTIALS_JSON={"type":"service_account",...}
   ```

4. **Deploy automatically** - Railway handles the rest!

## 🔧 Environment Variables

### Required
- `OPENAI_API_KEY` - Your OpenAI API key for AI insights
- `GOOGLE_CREDENTIALS_JSON` - Google service account credentials (as JSON string)

### Optional
- `FB_ACCESS_TOKEN` - Facebook Marketing API access token
- `FB_AD_ACCOUNT_ID` - Facebook ad account ID (format: act_123456789)

## 📊 Google Sheets Setup

### 1. Create Service Account
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create project and enable Google Sheets API
3. Create service account and download JSON credentials
4. Convert JSON to single line string for Railway environment variable

### 2. Share Your Sheets
Share these Google Sheets with your service account email:
- **Web Pages Data**: `1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA`
- **Attribution Data**: `1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ`
- **FB Spend Data**: `1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk`

## 🎯 Usage

### Dashboard
- View real-time performance metrics
- Monitor KPIs and success rates
- Track automation status

### Optimization
- Get AI-powered recommendations
- Review scaling/pausing suggestions
- Execute optimizations (with Facebook API)

### Creative Briefs
- Generate AI-enhanced creative briefs
- Based on winning performance patterns
- Seasonal vs Evergreen campaign strategies

### AI Insights
- Cluster analysis of successful ads
- Creative strategy recommendations
- Performance pattern recognition

## 🔄 Automation

The tool automatically:
- **Refreshes data** every 12 hours from Google Sheets
- **Runs AI analysis** daily at 9:00 AM
- **Generates insights** based on latest performance data
- **Updates recommendations** for optimization

## 🛡️ Security

- All API keys stored as environment variables
- No sensitive data in code repository
- Secure Google Sheets API integration
- Railway provides HTTPS by default

## 📈 Performance

- **Lightweight Flask application**
- **Optimized for Railway deployment**
- **Automatic scaling** based on usage
- **Fast AI-powered insights**

## 🆘 Support

### Common Issues

**1. Google Sheets Access Denied**
- Ensure service account email has Editor access to all sheets
- Verify GOOGLE_CREDENTIALS_JSON is properly formatted

**2. OpenAI API Errors**
- Check API key is valid and has credits
- Verify API usage limits

**3. Railway Deployment Issues**
- Check build logs in Railway dashboard
- Ensure all environment variables are set

### Getting Help
- Check Railway deployment logs
- Verify environment variables
- Test API connections individually

## 🚀 Next Steps

1. **Deploy to Railway** using this repository
2. **Configure environment variables** with your API keys
3. **Set up Google Sheets integration**
4. **Test the tool** with sample data
5. **Add Facebook API** for full automation
6. **Set up custom domain** (optional)

## 📝 License

This project is for internal use and optimization of Facebook advertising campaigns.

---

**🎯 Ready to optimize your Facebook ads with AI? Deploy now and start scaling your winners!**

