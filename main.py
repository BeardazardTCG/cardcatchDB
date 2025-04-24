# API Request Examples

## 1) Health Check
```bash
curl https://cardcatch-ebay-endpointnew.onrender.com/
# Expected Response:
# { "message": "CardCatch is live — production mode active." }
```

## 2) Single-Card Price Lookup
```bash
curl "https://cardcatch-ebay-endpointnew.onrender.com/price?card=Charizard&sandbox=false&limit=20"
# Expected Response:
# { "card":"Charizard","sold_count":20,"average_price":1158.17,"lowest_price":874.99,"highest_price":5000.0,"suggested_resale":1273.99 }
```

## 3) Bulk-Price Lookup
### Sandbox Stub Data (zeros):
```bash
curl -X POST "https://cardcatch-ebay-endpointnew.onrender.com/bulk-price?sandbox=true&limit=20" \
     -H "Content-Type: application/json" \
     -d '[
           { "card": "Charizard", "number": "SVP078" },
           { "card": "Pikachu" }
         ]'
# Expected Response (sandbox stub):
# [ { "card": "Charizard", "sold_count": 0, ... }, { "card": "Pikachu", "sold_count": 0, ... } ]
```

### Production Live Data:
```bash
curl -X POST "https://cardcatch-ebay-endpointnew.onrender.com/bulk-price?sandbox=false&limit=20" \
     -H "Content-Type: application/json" \
     -d '[
           { "card": "Charizard", "number": "SVP078" },
           { "card": "Pikachu" }
         ]'
# Expected Response:
# [
#   { "card":"Charizard","sold_count":20,... },
#   { "card":"Pikachu","sold_count":15,... }
# ]
```
