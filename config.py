SECRET_PROJECT_ID         = "certifyos-development"
SECRET_NAME               = "pdm-ops--service-account"

GEMINI_PROJECT_ID         = "certifyos-development"
GEMINI_LOCATION           = "us-central1"
GEMINI_MODEL              = "gemini-2.5-pro"

BQ_PROJECT                = "certifyos-production-platform"
BQ_DATASET                = "Humana_data_for_ops"
BQ_TABLE_ACTIVE           = f"{BQ_PROJECT}.{BQ_DATASET}.failed_roster_active"
BQ_TABLE_CHANGED          = f"{BQ_PROJECT}.{BQ_DATASET}.failed_roster_status_changed"
BQ_TABLE_HISTORY          = f"{BQ_PROJECT}.{BQ_DATASET}.run_history"
BQ_TABLE_PATTERNS         = f"{BQ_PROJECT}.{BQ_DATASET}.error_pattern_library"

CERTIFYOS_BASE_URL        = "https://api-service.certifyos.com"
CERTIFYOS_M2M_SECRET_NAME = "mtmnizprd"
TENANT_ID                 = "atod8ieaDHhPZrlAadMO"
FAILED_STATUS             = "FAILED"
PAGE_SIZE                 = 50
BATCH_SIZE                = 10

MAX_RETRIES               = 5
BASE_BACKOFF              = 2.0
MAX_BACKOFF               = 60.0

NPPES_API_URL             = "https://npiregistry.cms.hhs.gov/api/"

AUDIT_SECRET_HEADER       = "X-Audit-Secret"
AUDIT_SECRET_VALUE        = "change-me-in-cloud-run-env"

CORE_PREPROC_COLS = {
    "certifyos group name",
    "certifyos group npi",
    "certifyos group tin",
    "certifyos network name",
}

PLATFORM_KEYWORDS = (
    "runtimeexception",
    "retryabledalexception",
    "transactional batch execution",
    "nullpointerexception",
    "internalservererror",
    "500",
)

HUMANA_SEED_PATTERNS: list[tuple[str, str]] = [
    ("does not have a value in the enumeration",  "Degree / Title Not Valid"),
    ("not a recognised degree",                   "Degree / Title Not Valid"),
    ("not an recogonised degree",                 "Degree / Title Not Valid"),
    ("not an accepted license",                   "Degree / Title Not Valid"),
    ("licsw is not",                              "Degree / Title Not Valid"),
    ("practitioner title",                        "Degree / Title Missing"),
    ("title is required",                         "Degree / Title Missing"),
    ("npi is required should not be blank",       "NPI Blank"),
    ("npi is required",                           "NPI Blank"),
    ("first name is required",                    "First / Last Name Blank"),
    ("last name is required",                     "First / Last Name Blank"),
    ("humana: first name",                        "First / Last Name Blank"),
    ("humana: last name",                         "First / Last Name Blank"),
    ("provider name is not matching with nppes",  "NPPES Name Mismatch"),
    ("tin must be exactly",                       "TIN Error"),
    ("facility tin",                              "TIN Error"),
    ("exactly 9 digits",                          "TIN Error"),
    ("exactly 9 characters",                      "TIN Error"),
    ("geographic market",                         "Geographic Market Error"),
]
