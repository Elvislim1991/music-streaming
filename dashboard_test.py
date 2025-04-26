from pyspark.sql import SparkSession
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd
import psycopg2
import numpy as np

# Function to get the latest data from PostgreSQL
def get_latest_data(conn, minutes=5):
    """
    Get the latest data from PostgreSQL for visualization

    Args:
        conn: PostgreSQL connection
        minutes: Number of minutes of data to retrieve

    Returns:
        DataFrame with the latest data
    """
    # Calculate the time window
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)

    # SQL query to get the latest data from the 1-minute window metrics
    query = """
    SELECT time_window, 
           SUM(stream_count) as stream_count,
           AVG(skip_rate) as skip_rate,
           AVG(like_rate) as like_rate
    FROM user_engagement_metrics
    WHERE time_window >= %s AND time_window <= %s
    GROUP BY time_window
    ORDER BY time_window
    """

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query, (start_time, end_time))

    # Fetch the results
    results = cursor.fetchall()
    cursor.close()

    # Convert to pandas DataFrame
    df = pd.DataFrame(results, columns=['time_window', 'stream_count', 'skip_rate', 'like_rate'])
    return df

# Function to update the plot
def update_plot(frame, axes, conn):
    """
    Update the plot with the latest data

    Args:
        frame: Animation frame number
        axes: List of Matplotlib axes
        conn: PostgreSQL connection
    """
    # Get the latest data
    df = get_latest_data(conn)

    # If no data, return
    if df.empty:
        return

    # Get the time range for x-axis
    latest_time = df['time_window'].max()
    earliest_time = latest_time - timedelta(minutes=5)

    # Clear all axes
    for ax in axes:
        ax.clear()

    # Plot stream count on the first axis
    axes[0].plot(df['time_window'], df['stream_count'], 'b-')
    axes[0].set_title('Stream Count')
    axes[0].set_ylabel('Count')
    axes[0].grid(True)
    axes[0].set_xlim(earliest_time, latest_time)

    # Plot skip rate on the second axis
    axes[1].plot(df['time_window'], df['skip_rate'], 'r-')
    axes[1].set_title('Skip Rate')
    axes[1].set_ylabel('Rate')
    axes[1].grid(True)
    axes[1].set_xlim(earliest_time, latest_time)

    # Plot like rate on the third axis
    axes[2].plot(df['time_window'], df['like_rate'], 'g-')
    axes[2].set_title('Like Rate')
    axes[2].set_ylabel('Rate')
    axes[2].set_xlabel('Time')
    axes[2].grid(True)
    axes[2].set_xlim(earliest_time, latest_time)

    # Format the x-axis to show time
    plt.gcf().autofmt_xdate()

# Main function
def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="music_streaming",
        user="postgres",
        password="postgres"
    )

    # Create a figure with 3 subplots (one for each metric)
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    # Add a main title to the figure
    fig.suptitle('Real-time Music Streaming Dashboard', fontsize=16)

    # Create an animation that updates the plot every second
    ani = animation.FuncAnimation(fig, update_plot, fargs=(axes, conn), interval=1000)

    # Show the plot
    plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust for the main title
    plt.show()

    # Close the connection when done
    conn.close()

if __name__ == "__main__":
    main()
