import os
import pandas as pd
import re
import sys
import glob
import spacy
import traceback
import logging
from openai import AzureOpenAI
from datetime import datetime
import json
import math
import pyodbc
import configparser
import warnings
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

warnings.filterwarnings("ignore")

now=datetime.now()
date_string=now.strftime("%d_%m_%y_%H_%M_%S")

# --- Logging ---
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

for _old_log in glob.glob(os.path.join(LOG_DIR, 'service_auto_classification_*.log')):
    try:
        os.remove(_old_log)
    except OSError:
        pass

LOG_FILE = os.path.join(LOG_DIR, f'service_auto_classification_{date_string}.log')

logger = logging.getLogger('ServiceAutoClassification')
logger.setLevel(logging.INFO)
logger.propagate = False
_log_fmt = logging.Formatter(
    fmt='%(asctime)s %(levelname)-7s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
_file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
_file_handler.setFormatter(_log_fmt)
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_log_fmt)
logger.addHandler(_file_handler)
logger.addHandler(_console_handler)
logger.info(f'Log file: {LOG_FILE}')

# --- Config ---
config = configparser.ConfigParser(interpolation=None)
config.read("config.ini")

key = config['DEFAULT']['middleware_key']
No_of_ticket = config['DEFAULT']['No_of_ticket']
aes_key = base64.b64decode(key)
logger.info(f'No_of_ticket: {No_of_ticket}')

Driver = config['DATABASE']['Driver']
SQL_Server = config["DATABASE"]['SQL_Server']
Database = config['DATABASE']['Database']
User = config['DATABASE']['User']
pwd = config['DATABASE']['pwd']
connection_string = f'DRIVER={Driver};SERVER={SQL_Server};DATABASE={Database};UID={User};PWD={pwd}'


# --- DB connection (needed for mail SP) ---
cxn = pyodbc.connect(connection_string, autocommit=True)
logger.info('DB Connected')


# --- Mail notification ---
MAIL_ENVIRONMENT = config['MAIL']['Environment']
MAIL_SERVER_NAME = config['MAIL']['ServerName']
MAIL_JOB_NAME = config['MAIL']['JobName']
MAIL_FROM = config['MAIL']['FromMail']
MAIL_TO = config['MAIL']['ToMail']
MAIL_CC = config['MAIL']['CCMail']
MAIL_TEMPLATE_PATH = config['MAIL']['TemplatePath']
if not os.path.isabs(MAIL_TEMPLATE_PATH):
    MAIL_TEMPLATE_PATH = os.path.join(BASE_DIR, MAIL_TEMPLATE_PATH)

job_start_time = datetime.now()


def _format_dt(dt):
    return f'{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.minute:02d}:{dt.second:02d} UTC'


def _html_escape(value):
    if value is None:
        return ''
    return (str(value)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('\n', '<br>'))


with open(MAIL_TEMPLATE_PATH, 'r', encoding='utf-8') as _f:
    MAIL_TEMPLATE = _f.read()

ROW_TEMPLATE = (
    '<tr>'
    '<td width="160" style="padding: 6px 0; vertical-align: top;">{label}</td>'
    '<td width="15" style="padding: 6px 0; vertical-align: top;">:</td>'
    '<td style="padding: 6px 0; vertical-align: top;">{value}</td>'
    '</tr>'
)


def _render_mail(fields):
    rows_html = "\n".join(
        ROW_TEMPLATE.format(label=_html_escape(label), value=_html_escape(value))
        for label, value in fields
    )
    return MAIL_TEMPLATE.format(
        job_name=_html_escape(MAIL_JOB_NAME),
        rows=rows_html,
    )


JOB_STATUS_JOB_ID = 44
job_status_row_id = None


def _log_job_status(status, end_time=None):
    global job_status_row_id
    try:
        cursor = cxn.cursor()
        if status == 'Started':
            cursor.execute(
                """SET NOCOUNT ON;
                   INSERT INTO mas.JobStatus
                       (JobId, StartDateTime, EndDateTime, JobStatus, Remarks,
                        JobRunDate, InsertedRecordCount, DeletedRecordCount,
                        UpdatedRecordCount, IsDeleted, CreatedBy, CreatedDate)
                   VALUES (?, ?, ?, ?, NULL, GETDATE(), NULL, NULL, NULL, 0, ?, GETDATE());
                   SELECT CAST(SCOPE_IDENTITY() AS BIGINT);""",
                JOB_STATUS_JOB_ID, job_start_time, job_start_time, status, MAIL_JOB_NAME,
            )
            row = cursor.fetchone()
            job_status_row_id = int(row[0]) if row and row[0] is not None else None
            logger.info(f'JobStatus row inserted with Id={job_status_row_id}.')
        else:
            if job_status_row_id is None:
                logger.warning(f'No JobStatus row id captured; skipping {status} update.')
                cursor.close()
                return
            cursor.execute(
                """UPDATE mas.JobStatus
                   SET EndDateTime = ?, JobStatus = ?
                   WHERE Id = ? AND IsDeleted = 0""",
                end_time, status, job_status_row_id,
            )
            logger.info(f'JobStatus {status} logged for Id={job_status_row_id}.')
        cxn.commit()
        cursor.close()
    except Exception as log_err:
        logger.exception(f'Failed to log JobStatus for {status}: {log_err}')


def send_job_mail(status, exception_message=None):
    end_time = datetime.now()
    subject = f'{MAIL_ENVIRONMENT} - {MAIL_SERVER_NAME} - {MAIL_JOB_NAME} Job - {status}'

    fields = [
        ('Environment', MAIL_ENVIRONMENT),
        ('Server Name', MAIL_SERVER_NAME),
        ('Job Name', MAIL_JOB_NAME),
        ('Job Status', status),
        ('Started at', _format_dt(job_start_time)),
    ]
    if status != 'Started':
        fields.append(('Ended at', _format_dt(end_time)))
    if status == 'Failed':
        fields.append(('Exception Message', exception_message))

    body_html = _render_mail(fields)
    cc_value = MAIL_CC if MAIL_CC else None

    _log_job_status(status, end_time=end_time if status != 'Started' else None)

    try:
        cursor = cxn.cursor()
        cursor.execute(
            "EXEC [AVL].[SendDBEmail] @To=?, @From=?, @CC=?, @Subject=?, @Body=?",
            MAIL_TO, MAIL_FROM, cc_value, subject, body_html,
        )
        cxn.commit()
        cursor.close()
        logger.info(f'{status} mail sent via SP.')
    except Exception as mail_err:
        logger.exception(f'Failed to send {status} mail: {mail_err}')


def _failure_excepthook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, (SystemExit, KeyboardInterrupt)):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    err_text = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logger.error(f'Unhandled exception:\n{err_text}')
    send_job_mail('Failed', exception_message=err_text)
    sys.__excepthook__(exc_type, exc_value, exc_tb)


sys.excepthook = _failure_excepthook
send_job_mail('Started')


# --- Decryption ---
def decrypt_aes_gcm(aes_key: bytes, cipher_text: bytes) -> str:
    nonce_size = 12
    tag_size = 16

    nonce = cipher_text[:nonce_size]
    ciphertext = cipher_text[nonce_size:len(cipher_text) - tag_size]
    tag = cipher_text[len(cipher_text) - tag_size:]

    ciphertext_with_tag = ciphertext + tag

    aesgcm = AESGCM(aes_key)
    plaintext_bytes = aesgcm.decrypt(nonce, ciphertext_with_tag, None)

    return plaintext_bytes.decode("utf-8")


# --- Masking ---
nlp = spacy.load("en_core_web_lg")
STOP_WORDS = nlp.Defaults.stop_words


def remove_stop_words(text):
    return " ".join(tok for tok in text.split() if tok.lower() not in STOP_WORDS)


def spacy_masking(text):
    chunks = [text[i: i + 500000] for i in range(0, len(text), 500000)]
    docs = [nlp(chunk) for chunk in chunks]
    masked_tokens = []
    for doc in docs:
        for token in doc:
            if token.ent_type_ in ["PERSON", "ORG", "GPE"] and token.vector_norm.real >= 6.0:
                masked_tokens.append(f"[{token.ent_type_}]")
            else:
                masked_tokens.append(token.text)
    return " ".join(masked_tokens)


def sanitize_text_ph_mail(text):
    text = spacy_masking(text)
    mail_id_find = re.findall(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", text, re.IGNORECASE
    )
    for mail_id in mail_id_find:
        text = text.replace(mail_id, "[EMAIL]")

    website = re.findall(r"(https?://\S+|www\.\S+)", text, re.IGNORECASE)
    for site in website:
        text = text.replace(site, "[URL]")

    ip_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    ip_address = re.findall(ip_pattern, text, re.IGNORECASE)
    for ip in ip_address:
        text = text.replace(ip, "[IP]")

    phone_pattern = r"(?:(?:\+?\d{1,3}[-.])?(?:\(\d{1,3}\)|\d{1,3})[-.\s]?)?\d{2,4}[-.\s]?\d{2,4}[-.\s]?\d{2,4}"
    extension_pattern = r"(?:\s*(?:#|x|ext|ext.)\s*\d{1,6})?"
    pattern = f"{phone_pattern}{extension_pattern}"
    telephone_numbers = re.findall(pattern, text, re.MULTILINE | re.IGNORECASE)
    for phone in telephone_numbers:
        text = text.replace(phone, "[PHONE]")
    text = remove_stop_words(text)
    return text


# --- Load tickets ---
eligible_projects_query = '''SELECT ProjectId FROM [PP].[BestPractices] (NOLOCK)
WHERE IsGenAIEnabled=1 AND IsDeleted=0'''
eligible_projects = pd.read_sql(eligible_projects_query, cxn)['ProjectId'].tolist()
if not eligible_projects:
    logger.warning('No eligible projects found. Exiting.')
    sys.exit(0)
eligible_projects_csv = ','.join(str(p) for p in eligible_projects)
logger.info(f'Eligible projects: {eligible_projects_csv}')

ticket_query = config['QUERY']['TicketQuery'].replace('__PROJECTS__', eligible_projects_csv)
logger.debug(f'Ticket query: {ticket_query}')

df = pd.read_sql(ticket_query, cxn)
df.rename(columns={"TicketDescription": "Encrypted_Ticket_Desc"}, inplace=True)
df['Decrypted_Ticket_Desc'] = df['Encrypted_Ticket_Desc'].apply(
    lambda x: decrypt_aes_gcm(aes_key, base64.b64decode(x)) if (pd.notnull(x) and str(x).strip() != "") else ""
)
logger.info(f'{df.shape[0]} tickets loaded and decrypted')

# --- Apply masking ---
starttime = datetime.now()
df['Masked_Ticket_desc'] = df['Decrypted_Ticket_Desc'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
ticket_desc_time = datetime.now()
logger.info(f'Ticket description masking completed in {ticket_desc_time - starttime}')

df['Masked_ResolutionRemarks'] = df['ResolutionRemarks'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
resolution_remarks_time = datetime.now()
logger.info(f'Resolution remarks masking completed in {resolution_remarks_time - ticket_desc_time}')

df['Masked_TicketSummary'] = df['TicketSummary'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
ticketsummary_time = datetime.now()
logger.info(f'Ticket summary masking completed in {ticketsummary_time - resolution_remarks_time}')

df['Masked_Comments'] = df['Comments'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
comments_time = datetime.now()
logger.info(f'Comments masking completed in {comments_time - ticketsummary_time}')

df['Masked_FlexField1'] = df['FlexField1'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
flexfield1_time = datetime.now()
logger.info(f'FlexField1 masking completed in {flexfield1_time - comments_time}')

df['Masked_FlexField2'] = df['FlexField2'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
flexfield2_time = datetime.now()
logger.info(f'FlexField2 masking completed in {flexfield2_time - flexfield1_time}')

df['Masked_FlexField3'] = df['FlexField3'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
flexfield3_time = datetime.now()
logger.info(f'FlexField3 masking completed in {flexfield3_time - flexfield2_time}')

df['Masked_FlexField4'] = df['FlexField4'].apply(lambda x: sanitize_text_ph_mail(str(x)) if (pd.notnull(x) and x != "") else "")
flexfield4_time = datetime.now()
logger.info(f'FlexField4 masking completed in {flexfield4_time - flexfield3_time}')


# --- Azure OpenAI client ---
AZURE_OPENAI_ENDPOINT = config['AZURE']['AZURE_OPENAI_ENDPOINT']
OPENAI_API_VERSION = config['AZURE']['OPENAI_API_VERSION']
GPT_DEPLOYMENT_NAME = config['AZURE']['GPT_DEPLOYMENT_NAME']
OPENAI_API_KEY = config['AZURE']['OPENAI_API_KEY']

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=OPENAI_API_VERSION,
    api_key=OPENAI_API_KEY,
)


# --- Prediction ---
SYSTEM_PROMPT = """You are an expert IT Service Management (ITSM) analyst trained on the ITIL framework.
Your task is to analyze IT support ticket data and predict the most appropriate service
from a provided list of configured services.

Rules you MUST follow:
1. You will receive two inputs: a list of available services and a batch of ticket records.
2. You MUST only predict services that exist in the provided available services list.
3. Never invent or return a service name outside the configured services list.
4. Return predictions for ALL ticket IDs present in the input — no skipping.
5. Confidence score must be a float between 0.0 and 1.0.
6. Return ONLY a valid JSON object. No markdown, no explanation, no extra text."""

USER_PROMPT_TEMPLATE = """### Available Services:
{available_services}

### Ticket Data:
{ticket_json}

### Task:
For each ticket in the Ticket Data above:
1. Analyze ALL available fields: Masked_Ticket_desc, Masked_ResolutionRemarks,
   Masked_TicketSummary, Masked_Comments, Masked_FlexField1–4, Category, CauseCode, ResolutionCode.
2. Match the ticket context to the BEST fitting service from the Available Services list.
3. Prioritize signals in this order:
   - Primary   → Masked_Ticket_desc,  Masked_ResolutionRemarks
   - Secondary → CauseCode, ResolutionCode
   - Supporting → Masked_Comments, Masked_FlexField1–4,  Masked_TicketSummary, Category
4. If a field is empty or masked, ignore it and rely on remaining fields.
5. Assign a confidence score (0.0–1.0) reflecting how strongly the ticket matches
   the predicted service. Use lower scores when signals are weak or ambiguous.

### Output Format (strict JSON):
{{
  "0": {{"service": "<service from available list>", "confidence": <0.0–1.0>}},
  "1": {{"service": "<service from available list>", "confidence": <0.0–1.0>}},
  ...
}}

Return ONLY the JSON object. No explanation. No markdown. No text outside the JSON."""


def predict_services(available_services: list, tickets: dict) -> dict:
    user_prompt = USER_PROMPT_TEMPLATE.format(
        available_services=json.dumps(available_services, indent=2),
        ticket_json=json.dumps(tickets, indent=2)
    )

    response = client.chat.completions.create(
        model=GPT_DEPLOYMENT_NAME,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
    )

    raw_output = response.choices[0].message.content

    try:
        predictions = json.loads(raw_output)
    except json.JSONDecodeError as e:
        raise ValueError(f"Model returned invalid JSON: {e}\nRaw output:\n{raw_output}")

    invalid = [
        (ticket_id, pred["service"])
        for ticket_id, pred in predictions.items()
        if pred.get("service") not in available_services
    ]
    if invalid:
        logger.warning(f'Out-of-scope service predictions detected: {invalid}')

    return predictions


# --- Batch predict by TicketType ---
masked_df = df[['Masked_Ticket_desc', 'Masked_ResolutionRemarks', 'Masked_TicketSummary',
                'Masked_Comments', 'Masked_FlexField1', 'Masked_FlexField2',
                'Masked_FlexField3', 'Masked_FlexField4', 'Category', 'CauseCode',
                'ResolutionCode', 'TicketTypeMapID']]

final_result={}
for tickettype in masked_df['TicketTypeMapID'].unique():
    services_query = f'''Select MS.ServiceID, MS.ServiceName from [AVL].[TK_MAP_TicketTypeServiceMapping] TTM
    JOIN [AVL].[TK_MAS_Service] MS ON MS.ServiceID=TTM.ServiceID
    where Tickettypemappingid={tickettype} and TTM.IsDeleted=0 AND MS.IsDeleted=0'''
    services_df = pd.read_sql(services_query, cxn)
    available_service = list(services_df['ServiceName'])
    service_name_to_id = dict(zip(services_df['ServiceName'], services_df['ServiceID']))
    tickettype_df = masked_df[df['TicketTypeMapID'] == tickettype]
    for i in range(math.ceil(tickettype_df.shape[0] / 50)):
        api_input = tickettype_df[i * 50:(i + 1) * 50].to_json(orient='index')
        results = predict_services(available_service, api_input)
        for pred in results.values():
            pred['service_id'] = service_name_to_id.get(pred.get('service'))
        # with open(f"json_result_gpt41_{tickettype}_{i}.json", "w") as f:
        #     json.dump(results, f, indent=4, default=str)
        logger.info(f'TicketType {tickettype}: API batch {i+1} of {math.ceil(tickettype_df.shape[0] / 50)} completed')
        final_result.update(results)

df['PredictedService']=df.index.map(lambda x: final_result.get(str(x), {}).get('service') if final_result.get(str(x), {}).get('confidence', 0) > 0.4 else '')
df['PredictedServiceId']=df.index.map(lambda x: final_result.get(str(x), {}).get('service_id') if final_result.get(str(x), {}).get('confidence', 0) > 0.4 else '')

# df.to_csv(f"masked_data_{date_string}.csv", index=False)

# --- Update DB with predicted ServiceId ---
has_prediction = df['PredictedServiceId'].notna() & (df['PredictedServiceId'] != '')

predicted_rows = [
    (int(row['PredictedServiceId']), str(row['TicketID']), int(row['ProjectID']))
    for _, row in df[has_prediction].iterrows()
]
unpredicted_rows = [
    (str(row['TicketID']), int(row['ProjectID']))
    for _, row in df[~has_prediction].iterrows()
]

if predicted_rows:
    predicted_sql = '''UPDATE AVL.TK_TRN_TicketDetail
    SET ServiceId = ?,
        ServiceClassificationMode = 5,
        ModifiedBy = 'ServiceAutoClassificationJob'
    WHERE TicketId = ? AND ProjectId = ?
      AND (ServiceId IS NULL OR ServiceId = 0)'''
    cursor = cxn.cursor()
    cursor.fast_executemany = True
    cursor.executemany(predicted_sql, predicted_rows)
    cxn.commit()
    cursor.close()
    logger.info(f'{len(predicted_rows)} tickets updated with predicted ServiceId (Mode 1)')
else:
    logger.info('No tickets with predicted ServiceId to update.')

if unpredicted_rows:
    unpredicted_sql = '''UPDATE AVL.TK_TRN_TicketDetail
    SET ServiceClassificationMode = 3,
        ModifiedBy = 'ServiceAutoClassificationJob'
    WHERE TicketId = ? AND ProjectId = ?
      AND (ServiceId IS NULL OR ServiceId = 0)'''
    cursor = cxn.cursor()
    cursor.fast_executemany = True
    cursor.executemany(unpredicted_sql, unpredicted_rows)
    cxn.commit()
    cursor.close()
    logger.info(f'{len(unpredicted_rows)} tickets marked as unclassified (Mode 3)')
else:
    logger.info('No tickets to mark as unclassified.')

logger.info('Job done')
send_job_mail('Completed')
