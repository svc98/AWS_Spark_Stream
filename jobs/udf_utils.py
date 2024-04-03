import re
from datetime import datetime
from word2number import w2n

def extract_file_name(file_content):
    try:
        file_content = file_content.strip()
        position = file_content.split('\n')[0]
        return position
    except Exception as e:
        raise ValueError(f"Error extracting file name: {e}")
def extract_position(file_content):
    try:
        position_match = re.search(r"([Rr]ole [Tt]itle:)\s*(\w+\s?\w*)", file_content)
        position = position_match.group(2) if position_match else None
        return position
    except Exception as e:
        raise ValueError(f"Error extracting position: {e}")

def extract_job_code(file_content):
    try:
        classcode_match = re.search(
            r"JOB CODE:\s*(\d+)",
            file_content,
            re.IGNORECASE)
        classcode = classcode_match.group(1) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f"Error extracting job code: {e}")

def extract_salary(file_content):
    try:
        salary_pattern = re.search(
            r"\$(\d{1,3},\d{3})\s+(to|-)\s+\$(\d{1,3},\d{3});\s?\$?(\d{1,3},\d{3})?\s?(to|-)?\s?\$?(\d{1,3},\d{3})?;?\s?\$?(\d{1,3},\d{3})?\s?(to|-)?\s?\$?(\d{1,3},\d{3})?;?",
            file_content)
        lower_band, upper_band = None, None
        if salary_pattern:
            lower_band = float(salary_pattern.group(1).replace(',',''))
            if salary_pattern.group(9):
                upper_band = float(salary_pattern.group(9).replace(',',''))
            elif salary_pattern.group(6):
                upper_band = float(salary_pattern.group(6).replace(',',''))
            else:
                upper_band = float(salary_pattern.group(3).replace(',',''))
        return lower_band, upper_band
    except Exception as e:
        raise ValueError(f"Error extracting salary: {e}")

def extract_start_date(file_content):
    try:
        start_date_match = re.search(r"([Ss]tart [Dd]ate:)\s*(\d{1,2}-\d{1,2}-\d{4})", file_content)
        date = datetime.strptime(start_date_match.group(2), "%m-%d-%Y" ) if start_date_match else None
        return date
    except Exception as e:
        raise ValueError(f"Error extracting start date: {e}")

def extract_end_date(file_content):
    try:
        end_date_match = re.search(
            r"(January|Feburary|March|April|May|June|July|August|September|October|November|December)\s(\d{1,2},\s\d{4})",
            file_content)
        date = datetime.strptime(end_date_match.group(), "%B %d, %Y")
        return date
    except Exception as e:
        raise ValueError(f"Error extracting end date: {e}")

def extract_req(file_content):
    try:
        req_match = re.search(
            r"(JOB)?\s?REQUIREMENT(S)?:\n?(.*)",
            file_content,
            re.IGNORECASE)
        req = req_match.group(3).strip() if req_match else None
        return req
    except Exception as e:
        raise ValueError(f"Error extracting requirements: {e}")

def extract_notes(file_content):
    try:
        notes_match = re.search(
            r"(NOTE[S?]):(.*)",
            file_content,
            re.DOTALL | re.IGNORECASE)
        notes = notes_match.group(2).strip() if notes_match else None
        return notes
    except Exception as e:
        raise ValueError(f"Error extracting notes: {e}")

def extract_job_desc(file_content):
    try:
        job_desc_match = re.search(
            r"((ROLE|JOB) DESCRIPTION):\n(.*)",
            file_content,
            re.IGNORECASE)
        job_desc = job_desc_match.group(3).strip() if job_desc_match else None
        return job_desc
    except Exception as e:
        raise ValueError(f"Error extracting job description: {e}")

def extract_selection_process(file_content):
    try:
        selection_match = re.search(
            r"SELECTION(S)?\s?(PROCESS)?:(.*)",
            file_content,
            re.IGNORECASE)
        selection = selection_match.group(3).strip() if selection_match else None
        return selection
    except Exception as e:
        raise ValueError(f"Error extracting selection: {e}")

def extract_experience_length(file_content):
    try:
        experience_match = re.search(
            r"([0-9]|[0-3][0-9]|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\sYEAR[S?]\s?(OF)?(RELEVANT)?(EXPERIENCE)?",
            file_content,
            re.IGNORECASE)
        selection = experience_match.group(1).strip() if experience_match else None
        if not selection.isnumeric():
            selection = w2n.word_to_num(selection)
        return selection
    except Exception as e:
        raise ValueError(f"Error extracting experience length: {e}")

def extract_job_type(file_content):
    try:
        job_type_match = re.search(
            r"EMPLOYMENT TYPE:(.*)",
            file_content,
            re.IGNORECASE)
        job_type = job_type_match.group(1).strip() if job_type_match else None
        return job_type
    except Exception as e:
        raise ValueError(f"Error extracting job type: {e}")

def extract_education(file_content):
    try:
        education_match = re.search(
            r"(ASSOCIATE|BACHELOR|MASTER|DOCTORAL)'?S?\sDEGREE",
            file_content,
            re.IGNORECASE)
        education = education_match.group(1) if education_match else None
        return education
    except Exception as e:
        raise ValueError(f"Error extracting education: {e}")

def extract_application_location(file_content):
    try:
        education_match = re.search(
            r"(APPLICATION:)\s?(.*)",
            file_content,
            re.IGNORECASE)
        education = education_match.group(2).strip() if education_match else None
        return education
    except Exception as e:
        raise ValueError(f"Error extracting education: {e}")