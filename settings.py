import os
import dotenv

dotenv.load_dotenv()

RECIPES_URL = os.getenv("RECIPES_URL")
WORKDIR = os.getenv("WORKDIR", "/tmp")