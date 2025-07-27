import streamlit as st
import openair_api.openair_execute as oae
import openair_api.utils as u
import xml.etree.ElementTree as ET
from io import BytesIO
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
import random
import re
from dotenv import load_dotenv
import os

load_dotenv("email_config.env")

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
CODE_EXPIRY_MINUTES = int(os.getenv("CODE_EXPIRY_MINUTES", "3"))

# --- EMAIL VERIFICATION LOGIC ---

def is_valid_email(email: str) -> bool:
    email_regex = r"^[\w\.-]+@blend360\.com$"
    return re.match(email_regex, email) is not None

def generate_code() -> str:
    return str(random.randint(100000, 999999))

def send_verification_code(email: str, code: str):
    subject = "Blend360 Invoice Validator - Your Verification Code"
    body = f"Your verification code is: {code}"

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.send_message(msg)

    print(f"[DEBUG] Sent code {code} to {email}")

def handle_error_and_stop(message: str):
    st.error(message)
    st.stop()

def verify_email_flow():
    st.subheader("🔐 Email Verification")
    state = st.session_state

    for key in ["verified_emails", "verification_code", "verification_expiry", "verifying_email", "code_sent"]:
        if key not in state:
            state[key] = None if key != "verified_emails" else {}

    email = st.text_input("Enter your Blend360 email:").strip().lower()
    if not email:
        st.stop()
    if not is_valid_email(email):
        handle_error_and_stop("Please use a valid @blend360.com email.")

    if state["verified_emails"].get(email):
        return email

    if not state["code_sent"] or email != state["verifying_email"]:
        code = generate_code()
        send_verification_code(email, code)
        state["verification_code"] = code
        state["verification_expiry"] = datetime.utcnow() + timedelta(minutes=CODE_EXPIRY_MINUTES)
        state["verifying_email"] = email
        state["code_sent"] = True
        st.success(f"📧 Code sent to {email}. Please check your inbox.")

    code_input = st.text_input("Enter the 6-digit verification code:")
    if not code_input:
        st.info("⏳ Waiting for verification code...")
        st.stop()

    if datetime.utcnow() > state["verification_expiry"]:
        state["code_sent"] = False
        handle_error_and_stop("⏰ Code expired. Please reload and try again.")

    if code_input == state["verification_code"]:
        state["verified_emails"][email] = True
        st.success("✅ Email verified.")
        st.success("📬 Obtaining latest email-to-issuer company mapping, please wait...")
        st.rerun()
    else:
        handle_error_and_stop("❌ Incorrect code. Please try again.")

# --- INVOICE PARSER ---

def parse_xml(file):
    try:
        tree = ET.parse(BytesIO(file.read()))
        root = tree.getroot()
        ns = {"cfe": "http://cfe.dgi.gub.uy"}

        def get_text(path, field):
            el = root.find(path, ns)
            if el is None or el.text is None:
                raise ValueError(f"Missing or empty field: {field}")
            return el.text.strip()

        return {
            "description": f"{get_text('.//cfe:NomItem', 'NomItem')} {get_text('.//cfe:DscItem', 'DscItem')}",
            "amount_of_hours": float(get_text(".//cfe:Cantidad", "Cantidad")),
            "rate": float(get_text(".//cfe:PrecioUnitario", "PrecioUnitario")),
            "date": get_text(".//cfe:FchEmis", "FchEmis"),
            "ruc_emisor": get_text(".//cfe:RUCEmisor", "RUCEmisor"),
            "razon_social": get_text(".//cfe:RznSoc", "RznSoc"),
            "rut_receptor": get_text(".//cfe:DocRecep", "DocRecep"),
        }
    except Exception as e:
        handle_error_and_stop(f"XML parsing failed: {e}")

# --- UI Pages ---

page = st.sidebar.selectbox("Select Page", ["Invoice Checker", "Email to Invoice Issuer"])

if page == "Invoice Checker":
    st.title("📄 Invoice Checker")
    email = verify_email_flow()

    user_id = oae.get_user_id(email)
    if user_id is None:
        st.error("No user found with this email.")
        st.stop()

    mapping_df = u.load_mapping_csv()
    issuer = u.get_latest_issuer_for_email(mapping_df, email)
    if issuer is None:
        st.error("No issuer mapping found. Please update it.")
        st.stop()

    st.info(f"Mapped issuer: **{issuer}**")
    uploaded_file = st.file_uploader("Upload your invoice (XML file)")

    if uploaded_file:
        data = parse_xml(uploaded_file)
        st.write("Parsed Invoice:", data)

        if data["razon_social"].strip().lower() != issuer.lower():
            handle_error_and_stop(f"Issuer mismatch: invoice says '{data['razon_social']}', expected '{issuer}'")

        if data["rut_receptor"] != "217898170017":
            handle_error_and_stop(f"Invalid RUT receptor: {data['rut_receptor']}")

        required_keywords = [
            "desarrollo", "software", "exonerados",
            "irae", "literal s articulo 52 titulo 4"
        ]
        missing = [kw for kw in required_keywords if kw not in data["description"].lower()]
        if missing:
            handle_error_and_stop(f"Missing keywords: {', '.join(missing)}")

        st.success("🎉 All invoice quality checks passed successfully.")
        st.info("🔍 Comparing invoice hours vs OpenAir hours, please wait...")

        try:
            end_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
            start_date = end_date.replace(day=1)
            summary = u.get_summary(start_date, end_date, user_id)
            invoice_hours = data["amount_of_hours"]
            user_hours = summary.get(user_id)

            if user_hours is None:
                handle_error_and_stop("No OpenAir hours found.")

            st.write(f"🧾 Invoice hours: {invoice_hours} | 🕒 OpenAir hours: {user_hours}")

            if abs(invoice_hours - user_hours) < 0.01:
                st.success("✅ Hours match.")
                success, error = u.process_invoice(data, email, end_date)
                if success:
                    st.success("✅ File uploaded to S3.")
                else:
                    handle_error_and_stop(f"Upload failed: {error}")
            else:
                handle_error_and_stop("Mismatch in hours.")
        except Exception as e:
            handle_error_and_stop(f"Error comparing hours: {e}")

elif page == "Email to Invoice Issuer":
    st.title("🔁 Email-Issuer Mapper")

    if st.session_state.get("mapping_saved"):
        st.success(st.session_state["mapping_saved"])
        del st.session_state["mapping_saved"]

    email = verify_email_flow()
    issuer = st.text_input("Issuer company name:")

    if st.button("Add / Update mapping"):
        if not issuer:
            st.error("Please enter an issuer name.")
        else:
            mapping_df = u.load_mapping_csv()
            mapping_df = u.add_mapping(mapping_df, email, issuer.strip())
            u.save_mapping_csv(mapping_df)
            st.session_state["mapping_saved"] = f"✅ Mapping saved: {email} → {issuer.strip()}"
            st.rerun()
