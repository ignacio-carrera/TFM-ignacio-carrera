# tests/test_app_employees.py

import sys
from io import BytesIO
from unittest.mock import patch, MagicMock

# Mock external modules before import
sys.modules["streamlit"] = MagicMock()
sys.modules["smtplib"] = MagicMock()

import app_employees as ae


# --- VALID XML SAMPLE ---
VALID_XML = b"""
<CFE xmlns="http://cfe.dgi.gub.uy">
    <Encabezado>
        <Emisor>
            <RUCEmisor>123456789012</RUCEmisor>
            <RznSoc>Mi Empresa S.A.</RznSoc>
        </Emisor>
        <Receptor>
            <DocRecep>217898170017</DocRecep>
        </Receptor>
    </Encabezado>
    <Detalle>
        <Cantidad>40</Cantidad>
        <PrecioUnitario>1000</PrecioUnitario>
        <NomItem>Desarrollo</NomItem>
        <DscItem>Software exonerados literal S articulo 52 titulo 4</DscItem>
    </Detalle>
    <FchEmis>2024-06-01</FchEmis>
</CFE>
"""


# --- TEST CASES ---

def test_is_valid_email():
    assert ae.is_valid_email("someone@blend360.com")
    assert not ae.is_valid_email("someone@gmail.com")
    assert not ae.is_valid_email("not-an-email")


@patch("app_employees.st.error")
@patch("app_employees.st.stop")
def test_handle_error_and_stop(mock_stop, mock_error):
    ae.handle_error_and_stop("Error!")
    mock_error.assert_called_once_with("Error!")
    mock_stop.assert_called_once()


def test_parse_xml_valid():
    file = BytesIO(VALID_XML)
    result = ae.parse_xml(file)
    assert result["razon_social"] == "Mi Empresa S.A."
    assert result["ruc_emisor"] == "123456789012"
    assert result["rut_receptor"] == "217898170017"
    assert result["rate"] == 1000.0
    assert result["amount_of_hours"] == 40.0
    assert "desarrollo" in result["description"].lower()


@patch("app_employees.smtplib.SMTP_SSL")
@patch("app_employees.GMAIL_USER", "mockuser@blend360.com")
@patch("app_employees.GMAIL_PASSWORD", "mockpassword")
def test_send_verification_code(mock_smtp_ssl):
    mock_server = mock_smtp_ssl.return_value.__enter__.return_value
    ae.send_verification_code("test@blend360.com", "123456")

    mock_server.login.assert_called_once_with("mockuser@blend360.com", "mockpassword")
    mock_server.send_message.assert_called_once()
