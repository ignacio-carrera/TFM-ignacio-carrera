from datetime import datetime
import streamlit as st
import time
import boto3
import pandas as pd
import altair as alt
from pyathena import connect

import openair_api.openair_execute as oae
import openair_api.utils as u

# ----------------------
# Athena Config
# ----------------------
ATHENA_DB = "openair"
S3_OUTPUT = "s3://aws-glue-assets-376465608000-us-east-2/athena_results/"
ATHENA_REGION = "us-east-2"
WORKGROUP = "primary"

# ----------------------
# Athena Loader
# ----------------------
@st.cache_data(show_spinner=False)
def load_athena_table(table_name):
    conn = connect(
        s3_staging_dir=S3_OUTPUT,
        region_name=ATHENA_REGION,
        work_group=WORKGROUP
    )
    query = f"SELECT * FROM {ATHENA_DB}.{table_name}"
    return pd.read_sql(query, conn)

# ----------------------
# Step Function Helpers
# ----------------------
STEP_FUNCTION_ARN = "arn:aws:states:us-east-2:376465608000:stateMachine:openair_data_pipeline"
REGION = "us-east-2"

def trigger_step_function():
    client = boto3.client("stepfunctions", region_name=REGION)
    execution_name = "data_pipeline_execution_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    response = client.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=execution_name,
        input='{"job_name": "landing_to_raw"}'
    )
    return response["executionArn"]

def wait_for_step_function_completion(execution_arn):
    client = boto3.client("stepfunctions", region_name=REGION)
    status = "RUNNING"
    with st.spinner("Waiting for Step Function to complete..."):
        while status == "RUNNING":
            response = client.describe_execution(executionArn=execution_arn)
            status = response["status"]
            time.sleep(5)
    return status

# ----------------------
# Page Selector
# ----------------------
page = st.sidebar.selectbox("Select Page", ["ETL", "Metrics Dashboard"])

# ----------------------
# Hydration Page
# ----------------------
if page == "ETL":
    st.title("🔄 ETL Module")

    start_date = st.date_input("Enter start date")
    end_date = st.date_input("Enter end date")

    if st.button("Run billable hours and users hydration"):
        st.info("Obtaining data from OpenAir...")
        summary = u.get_summary(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        users = oae.get_users()
        summary_csv = u.transform_summary_to_csv(summary, start_date, end_date)
        user_csv = u.transform_users_to_csv(users)

        u.put_in_s3(user_csv, "tfm-bucket-openair", "landing/users/", "users_summary")
        u.put_in_s3(summary_csv, "tfm-bucket-openair", "landing/billable_hours_summary/", "billable_hours_summary")
        st.success("OpenAir data uploaded to S3 successfully ✅")

        st.write("Triggering Step Function for data hydration...")
        execution_arn = trigger_step_function()
        final_status = wait_for_step_function_completion(execution_arn)

        if final_status == "SUCCEEDED":
            st.success("Finance hydration completed successfully ✅")
        elif final_status == "FAILED":
            st.error("Finance hydration execution failed ❌")
        else:
            st.warning(f"Step Function finished with status: {final_status}")

# ----------------------
# Dashboard Page
# ----------------------
elif page == "Metrics Dashboard":
    st.title("📊 Metrics Dashboard")

    # Load data from Athena
    try:
        users_df = load_athena_table("users_std")
        hours_df = load_athena_table("billable_hours_summary_std")
    except Exception as e:
        st.error(f"Failed to load data from Athena: {e}")
        st.stop()

    if users_df.empty or hours_df.empty:
        st.warning("No data available. Please hydrate first.")
        st.stop()

    # Merge to attach user email
    merged_df = hours_df.merge(
        users_df[["id", "email"]],
        left_on="userid",
        right_on="id",
        how="left"
    )

    # Friendly user display
    merged_df["display_user"] = merged_df["email"].fillna(merged_df["userid"])

    # KPIs
    st.subheader("🔢 Summary Metrics")
    col1, col2 = st.columns(2)
    col1.metric("Total Users", users_df["id"].nunique())
    col2.metric("Total Billable Hours", round(hours_df["total_billable_hours"].sum(), 2))

    # User selection
    selected_user = st.selectbox("Select a user", sorted(merged_df["display_user"].dropna().unique()))
    user_hours_df = merged_df[merged_df["display_user"] == selected_user]

    if not user_hours_df.empty:
        # Chart: Hours over Time
        st.subheader("⏳ Billable Hours Evolution")
        chart = alt.Chart(user_hours_df).mark_line(point=True).encode(
            x="start_date:T",
            y="total_billable_hours:Q",
            tooltip=["start_date", "end_date", "total_billable_hours"]
        ).properties(height=400, width=700)
        st.altair_chart(chart, use_container_width=True)

        # Raw Table
        st.subheader("📋 Raw Records")
        st.dataframe(user_hours_df)
    else:
        st.info("No data available for selected user.")
