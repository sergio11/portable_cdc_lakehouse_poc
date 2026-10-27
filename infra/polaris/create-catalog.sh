#!/bin/sh
# Create the `pcl` catalog in Apache Polaris if it does not exist yet.
#
# Polaris stores only metadata. Data files live in MinIO and Beam, Spark and
# Trino keep their own S3 credentials, so `stsUnavailable=true` is correct here:
# MinIO does not implement STS and credential vending is not needed.
#
# Deliberately dependency-free: the container ships only curl and a POSIX shell.
# Installing `jq` at run time made catalog bootstrap depend on Alpine mirrors and
# failed on transient DNS errors, so the small amount of JSON handling below is
# done with sed and a heredoc instead.
set -eu

REALM="POLARIS"
BASE_URL="http://polaris:8181"
CATALOG="pcl"
BUCKET="${PCL_ICEBERG_BUCKET:?PCL_ICEBERG_BUCKET is required}"
REGION="${PCL_MINIO_REGION:-us-east-1}"
BASE_LOCATION="s3://${BUCKET}/warehouse/"
ENDPOINT="http://minio:9000"

# These values are interpolated into JSON. They come from .env, so reject
# anything that could break the payload instead of emitting invalid JSON.
for value in "${CATALOG}" "${BUCKET}" "${REGION}"; do
  case "${value}" in
    *[!A-Za-z0-9._-]*)
      echo "Unsafe catalog configuration value: ${value}" >&2
      exit 1
      ;;
  esac
done

echo "Requesting a Polaris access token..."
TOKEN_RESPONSE=$(curl --fail-with-body -sS \
  --retry 20 --retry-delay 2 --retry-all-errors \
  "${BASE_URL}/api/catalog/v1/oauth/tokens" \
  --user "${PCL_POLARIS_CLIENT_ID}:${PCL_POLARIS_CLIENT_SECRET}" \
  -H "Polaris-Realm: ${REALM}" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL)

TOKEN=$(printf '%s' "${TOKEN_RESPONSE}" | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

if [ -z "${TOKEN}" ]; then
  echo "Could not obtain a Polaris access token. Response: ${TOKEN_RESPONSE}" >&2
  exit 1
fi

STATUS=$(curl -sS -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${REALM}" \
  "${BASE_URL}/api/management/v1/catalogs/${CATALOG}")

if [ "${STATUS}" = "200" ]; then
  echo "Polaris catalog '${CATALOG}' already exists."
elif [ "${STATUS}" = "404" ]; then
  PAYLOAD=$(cat <<JSON
{
  "catalog": {
    "name": "${CATALOG}",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "${BASE_LOCATION}",
      "polaris.config.drop-with-purge.enabled": "true"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["${BASE_LOCATION}"],
      "endpoint": "${ENDPOINT}",
      "endpointInternal": "${ENDPOINT}",
      "pathStyleAccess": true,
      "region": "${REGION}",
      "stsUnavailable": true
    }
  }
}
JSON
)

  curl --fail-with-body -sS \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Polaris-Realm: ${REALM}" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/management/v1/catalogs" \
    -d "${PAYLOAD}" >/dev/null

  echo "Created Polaris catalog '${CATALOG}' at ${BASE_LOCATION}."
else
  echo "Unexpected Polaris response while checking catalog: HTTP ${STATUS}" >&2
  exit 1
fi

# `catalog_admin` carries CATALOG_MANAGE_ACCESS and CATALOG_MANAGE_METADATA but
# not CATALOG_MANAGE_CONTENT. Without content management Polaris refuses
# DROP_TABLE_WITH_PURGE (it requires TABLE_DROP plus TABLE_WRITE_DATA), so the
# first replay works because the target does not exist yet and the second one
# fails while dropping it. Granting content management at catalog scope makes
# every namespace and table inherit it. The grant is applied on every start, so
# an existing catalog created before this fix is repaired too.
GRANTS=$(curl --fail-with-body -sS \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${REALM}" \
  "${BASE_URL}/api/management/v1/catalogs/${CATALOG}/catalog-roles/catalog_admin/grants")

if printf '%s' "${GRANTS}" | grep -q '"privilege":"CATALOG_MANAGE_CONTENT"'; then
  echo "Polaris catalog role 'catalog_admin' already holds CATALOG_MANAGE_CONTENT."
else
  curl --fail-with-body -sS -X PUT \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Polaris-Realm: ${REALM}" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/management/v1/catalogs/${CATALOG}/catalog-roles/catalog_admin/grants" \
    -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' >/dev/null

  echo "Granted CATALOG_MANAGE_CONTENT to Polaris catalog role 'catalog_admin'."
fi

# Replay drops tables with purge, but Polaris rejects the purge unless the
# feature is enabled on the catalog. The setting lives in catalog properties so
# it is scoped to this POC instead of the whole server. An existing catalog is
# updated in place, which keeps the bootstrap idempotent.
CATALOG_JSON=$(curl --fail-with-body -sS \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${REALM}" \
  "${BASE_URL}/api/management/v1/catalogs/${CATALOG}")

if printf '%s' "${CATALOG_JSON}" | grep -q '"polaris.config.drop-with-purge.enabled":"true"'; then
  echo "Polaris catalog '${CATALOG}' already enables drop-with-purge."
else
  VERSION=$(printf '%s' "${CATALOG_JSON}" | sed -n 's/.*"entityVersion"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p')

  if [ -z "${VERSION}" ]; then
    echo "Could not read the catalog entity version from: ${CATALOG_JSON}" >&2
    exit 1
  fi

  UPDATE=$(cat <<JSON
{
  "currentEntityVersion": ${VERSION},
  "properties": {
    "default-base-location": "${BASE_LOCATION}",
    "polaris.config.drop-with-purge.enabled": "true"
  }
}
JSON
  )

  curl --fail-with-body -sS -X PUT \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Polaris-Realm: ${REALM}" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/management/v1/catalogs/${CATALOG}" \
    -d "${UPDATE}" >/dev/null

  echo "Enabled drop-with-purge on Polaris catalog '${CATALOG}'."
fi

