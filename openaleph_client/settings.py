import os

# OpenAleph client API settings

def get_env_var(env_var_name, default_value):
    for complete_env_var_name in [f"MEMORIOUS_{env_var_name}", f"OPENALEPH_{env_var_name}", f"OPAL_{env_var_name}", env_var_name]:
        if os.environ.get(complete_env_var_name):
            return os.environ.get(complete_env_var_name)
    
    return default_value

HOST = get_env_var("HOST", "localhost")
API_KEY = get_env_var("API_KEY", None)
INVENTORY_TABLE_NAME = get_env_var("INVENTORY_TABLE_NAME", "files")
DATABASE_URI = get_env_var("DATABASE_URI", "postgresql://opal:opal@localhost:54321/opal")

MAX_TRIES = int(os.environ.get("OPENALEPH_MAX_TRIES", 5))
MEMORIOUS_RATE_LIMIT = int(os.environ.get("OPENALEPH_MEMORIOUS_RATE_LIMIT", 120))
FILE_BATCH_SIZE = int(os.environ.get("OPENALEPH_FILE_BATCH_SIZE", 2))