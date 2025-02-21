import pandas as pd
import streamlit as st
import plotly.graph_objects as go

def generate_sentiment_heatmap(df: pd.DataFrame):
    """
    Generates an Excel-style annotated sentiment heatmap that includes:
      - A sentiment label on the first line and its numeric score (formatted to 2 decimals) on a new line.
      - A multi-select sidebar to choose brands.
      - A built-in Plotly date range slider on the x-axis.
      - Y-axis (brand names) styled in a bright white font on a dark theme.
    """
    # Ensure 'date' is in datetime format
    df["date"] = pd.to_datetime(df["date"])
    
    # Sidebar for multiple brand selections
    st.sidebar.header("Analysis Controls")
    brands = sorted(df["brand"].unique())
    selected_brands = st.sidebar.multiselect(
        "Select Brands",
        brands,
        default=brands[:min(2, len(brands))]
    )
    
    if not selected_brands:
        st.warning("Select at least one brand")
        return
    
    # Filter data for the selected brands
    filtered_data = df[df["brand"].isin(selected_brands)].copy()
    
    # Aggregate average sentiment scores by date and brand
    agg_data = filtered_data.groupby([filtered_data["date"].dt.date, "brand"])["sentiment"] \
                             .mean().reset_index()
    
    # Pivot the data so that rows are brands and columns are dates
    pivot_data = agg_data.pivot(index="brand", columns="date", values="sentiment")
    pivot_data = pivot_data.reindex(sorted(pivot_data.columns), axis=1)
    
    # Function to map sentiment score to a label
    def sentiment_label(score):
        if pd.isna(score):
            return ""
        if score >= 0.1:
            return "Positive"
        elif score <= -0.1:
            return "Negative"
        else:
            return "Neutral"
    
    # Create annotation text with the label on one line and the score on the next
    def annotation_text(score):
        if pd.isna(score):
            return ""
        label = sentiment_label(score)
        return f"{label}<br>{score:.2f}"
    
    # Create text annotations for each cell
    text_labels = pivot_data.applymap(annotation_text).values
    
    # Create the annotated heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=[str(date) for date in pivot_data.columns],
        y=pivot_data.index.tolist(),
        colorscale="RdYlGn",  # Red for negative, yellow for neutral, green for positive
        colorbar=dict(
            title="Sentiment",
            tickmode="array",
            tickvals=[-1, 0, 1],
            ticktext=["Negative", "Neutral", "Positive"]
        ),
        text=text_labels,
        texttemplate="%{text}",
        hoverongaps=False
    ))
    
    # Update layout with a built-in date range slider and enhanced labels.
    # We use the "plotly_dark" template so that the white brand labels are clearly visible.
    fig.update_layout(
        title=f"📊 Sentiment Trend Over Time for {', '.join(selected_brands)}",
        xaxis_title="Date",
        yaxis_title="Brand",
        height=600,
        template="plotly_dark",
        hovermode="closest",
        xaxis=dict(
            rangeslider=dict(visible=True),
            tickangle=45
        )
    )
    
    # Make the brand names (y-axis tick labels) larger and bright white.
    fig.update_yaxes(tickfont=dict(size=18, color="white", family="Arial Black"))
    
    # Display the chart in Streamlit
    st.plotly_chart(fig)
