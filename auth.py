from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

# This should match the tokenUrl from your login endpoint
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"

def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
            )
        # You can query your DB here and return the full user object
        return {"id": user_id}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )


async def get_current_user_dev(x_user_id: str | None = Header(None, alias="X-User-Id")):
    if not x_user_id:
        raise HTTPException(status_code=401, detail="X-User-Id header missing")
    # Return the UID as your "user identity" for now
    return {"id": x_user_id, "firebase_uid": x_user_id}
