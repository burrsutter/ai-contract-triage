
## ai-contract-triage

This is a follow-up to https://github.com/burrsutter/ai-message-triage

```
./reset.sh
```

```
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --list 
```

```
documents
document-review
contracts
structured-contracts
patients
structured-patients
reports
structured-reports
```

```
python3.11 -m venv venv
```

```
source venv/bin/activate
```

```
python -m intake.file-intake
```

```
python -m router.document-router
```

```
python -m structure-invoice.message-structure
```


```
python -m structure-patient.message-structure
```

Note: requires a llama stack server and has a system dependency on poppler

```
brew install poppler
```

```
python -m structure-patient-vision.message-structure
```

```
cd data\sample
```

```
curl -L -O "https://www.das.nh.gov/purchasing/docs/Notice_Of_Contract_SIGNED/8002969%20Salesforce%20Professional%20Services%20rev.pdf"
mv 8002969%20Salesforce%20Professional%20Services%20rev.pdf new-hampshire-crm.pdf
```

```
curl -L -O "https://bgs.vermont.gov/sites/bgs/files/files/purchasing-contracting/C-two/38298%202-2.pdf"
mv 38298%202-2.pdf vermont-crm.pdf
```

```
curl -L -O "https://www.michigan.gov/dtmb/-/media/Project/Websites/dtmb/Procurement/Contracts/MiDEAL-Media/001/071b6600108.pdf"
mv 071b6600108.pdf michigan-crm.pdf
```

```
curl -L -O "https://procurement.maryland.gov/wp-content/uploads/sites/12/2023/08/DGS-OSP-ICPA-POD-for-Salesforce-Licenses-08.2023-signed.pdf"
mv DGS-OSP-ICPA-POD-for-Salesforce-Licenses-08.2023-signed.pdf maryland-crm.pdf
```

```
curl -L -O "https://dir.texas.gov/sites/default/files/DIR%20BP2017-03%20Salesforce.pdf"
mv DIR%20BP2017-03%20Salesforce.pdf texas-crm.pdf
```

```
cp Sample_PDF_1.pdf ../intake
```

