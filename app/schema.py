from pydantic import BaseModel,Field
from typing import List, Optional, Dict, Any

class LoginRequest(BaseModel):
    username: str
    password: str
