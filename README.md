# Twitter Brand Engagement Analysis

Tweets (with metadata) are first retrieved and preprocessed for cleanliness and structure. Brand-specific data is then extracted, followed by advanced sentiment and contextual analyses, alongside tracking brand mention frequency. A “hyper-smart” engagement score (via models like **RandomForestRegressor**) is calculated using both metadata and sentiment outputs, forming the basis of a time-series trend analysis. **Facebook Prophet** is employed to forecast future engagement trends and enable predictive analytics. Finally, results are visualized in a **Streamlit** dashboard, showcasing engagement trends, sentiment distribution, brand mentions, a recommendation engine, and a 30-day engagement forecast.

## Key Steps

1. **Data Retrieval and Preprocessing**  
   - Collect tweets with associated metadata.  
   - Clean and structure the data to ensure consistency.

2. **Brand-Specific Extraction**  
   - Isolate data and metadata referencing specific brands.  

3. **Advanced Analyses**  
   - Conduct sentiment and contextual analyses.  
   - Track mention frequency for each brand over defined time periods.

4. **Engagement Scoring**  
   - Use **RandomForestRegressor** to build a hyper-smart engagement score.  
   - Combine metadata and sentiment outputs for a comprehensive model.

5. **Time-Series Analysis**  
   - Leverage historical engagement data for in-depth trend analysis.

6. **Forecasting**  
   - Employ **Facebook Prophet** to predict future engagement trends.  
   - Enhance predictive analytics capabilities with time-series modeling.

7. **Dashboard Visualization**  
   - Implement a **Streamlit** dashboard to display:  
     - Engagement trends over time  
     - Sentiment distribution  
     - Brand mention counts  
     - Decision recommendation engine by brand  
     - 30-day engagement forecast  

---

**Disclaimer:**  
This project focuses on analyzing and forecasting brand engagement trends on Twitter. All steps, from data collection to visualization, are intended as a framework for sentiment analysis, engagement scoring, and predictive modeling.
