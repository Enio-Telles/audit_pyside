## 2024-05-24 - Masking Passwords in Config APIs
**Vulnerability:** Cleartext passwords from .env file were exposed through /api/oracle/config GET endpoint to pre-fill frontend UI.
**Learning:** Sending cleartext passwords to frontend configuration UIs creates a major credential exposure risk, even for local tools.
**Prevention:** Mask passwords with a dummy string (e.g. "********") when serving them to the client, and explicitly ignore this mask string when the frontend submits updates to save, preserving the original password securely on the server.
