#!/bin/bash
# deploy.sh
set -a
source .env
set +a

sam deploy \
  --parameter-overrides \
    "AlphaVantageApiKey=${ALPHA_VANTAGE_API_KEY}" \
    "AlphaVantageApiKey1=${ALPHA_VANTAGE_API_KEY_1}" \
    "AlphaVantageApiKey2=${ALPHA_VANTAGE_API_KEY_2}" \
    "AlphaVantageApiKey3=${ALPHA_VANTAGE_API_KEY_3}" \
    "AlphaVantageApiKey4=${ALPHA_VANTAGE_API_KEY_4}" \
    "AlphaVantageApiKey5=${ALPHA_VANTAGE_API_KEY_5}" \
    "AlphaVantageApiKey6=${ALPHA_VANTAGE_API_KEY_6}" \
    "AlphaVantageApiKey7=${ALPHA_VANTAGE_API_KEY_7}" \
    "AlphaVantageApiKey8=${ALPHA_VANTAGE_API_KEY_8}" \
    "AlphaVantageApiKey9=${ALPHA_VANTAGE_API_KEY_9}" \
    "AlphaVantageApiKey10=${ALPHA_VANTAGE_API_KEY_10}" \
    "AlphaVantageApiKey11=${ALPHA_VANTAGE_API_KEY_11}" \
    "AlphaVantageApiKey12=${ALPHA_VANTAGE_API_KEY_12}" \
    "AlphaVantageApiKey13=${ALPHA_VANTAGE_API_KEY_13}" \
    "AlphaVantageApiKey14=${ALPHA_VANTAGE_API_KEY_14}" \
    "AlphaVantageApiKey15=${ALPHA_VANTAGE_API_KEY_15}"