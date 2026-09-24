# Credentials

Drop service-account JSON keys here. The folder is git-ignored.

## Required: `google-sa.json`

Single Google Cloud service account used for both:
- **Vision API** (Discord chart OCR)
- **Vertex AI** (Gemini-based alert parser)

Service account: `your-sa@your-gcp-project.iam.gserviceaccount.com`

Roles granted:
- `roles/serviceusage.serviceUsageConsumer`
- `roles/aiplatform.user`

`start.sh` automatically exports `GOOGLE_APPLICATION_CREDENTIALS` to point here.

## Re-issuing the key

```bash
gcloud iam service-accounts keys create ./keys/google-sa.json \
  --iam-account="your-sa@your-gcp-project.iam.gserviceaccount.com"
```

Old keys can be rotated/revoked via:

```bash
gcloud iam service-accounts keys list \
  --iam-account="your-sa@your-gcp-project.iam.gserviceaccount.com"
gcloud iam service-accounts keys delete <KEY_ID> \
  --iam-account="your-sa@your-gcp-project.iam.gserviceaccount.com"
```

## Moving to a new machine

1. Clone the repo on the new box
2. Copy `keys/google-sa.json` into the new repo's `keys/` folder
3. Run `./start.sh` — credentials are picked up automatically

No env vars to set, no paths to remember.
