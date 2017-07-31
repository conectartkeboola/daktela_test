<?php
// parametry importované z konfiguračního JSON řetězce v definici PHP aplikace v KBC

$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// parametry importované z konfiguračního JSON v KBC
$integrityValidationOn  = $config["parameters"]["integrityValidationOn"];
$callsIncrementalOutput = $config["parameters"]["callsIncrementalOutput"];
$diagOutOptions         = $config["parameters"]["diagOutOptions"];          // diag. výstup do logu Jobs v KBC - klíče: basicStatusInfo, jsonParseInfo, basicIntegrInfo, detailIntegrInfo
$adhocDump              = $config["parameters"]["adhocDump"];               // diag. výstup do logu Jobs v KBC - klíče: active, idFieldSrcRec

// full load / incremental load výstupní tabulky 'calls'
$incrementalOn = !empty($callsIncrementalOutput['incrementalOn']) ? true : false;   // vstupní hodnota false se vyhodnotí jako empty :)

// za jak dlouhou historii [dny] se generuje inkrementální výstup (0 = jen za aktuální den, 1 = od včerejšího dne včetně [default], ...)
$jsonHistDays   = $callsIncrementalOutput['incremHistDays'];
$incremHistDays = $incrementalOn && !empty($jsonHistDays) && is_numeric($jsonHistDays) ? $jsonHistDays : 1;
/* import parametru z JSON řetězce v definici Customer Science PHP v KBC:
    {
      "integrityValidationOn": true,
      "callsIncrementalOutput": {
        "incrementalOn": false,
        "incremHistDays": 1200
      },
      "diagOutOptions": {
        "basicStatusInfo": true,
        "jsonParseInfo": false,
        "basicIntegrInfo": true,
        "detailIntegrInfo": true
      },
      "adhocDump": {
        "active": false,
        "idFieldSrcRec": "301121251"
      }
   }
  -> podrobnosti viz https://developers.keboola.com/extend/custom-science
*/