<?php
// parametry importované z konfiguračního JSON řetězce v definici PHP aplikace v KBC

$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// parametry importované z konfiguračního JSON v KBC
$confParam = $config["parameters"];
$integrityValidationOn  = $confParam["integrityValidationOn"];
$processedInstances     = $confParam["processedInstances"]; // pole s údaji, které instance mají být zpracovány - např. ["1" => true, "2" => true, ...]
$incrementalMode        = $confParam["incrementalMode"];
$diagOutOptions         = $confParam["diagOutOptions"];     // diag. výstup do logu Jobs v KBC - klíče: basicStatusInfo, jsonParseInfo, basicIntegrInfo, detailIntegrInfo
$adhocDump              = $confParam["adhocDump"];          // diag. výstup do logu Jobs v KBC - klíče: active, idFieldSrcRec

// parametry inkrementálního režimu
$incrementalOn = empty($incrementalMode['incrementalOn']) ? false : true;   // vstupní hodnota false se vyhodnotí jako empty :)
$incrCallsOnly = empty($incrementalMode['incrCallsOnly']) ? false : true;   // vstupní hodnota false se vyhodnotí jako empty :)
$histDays      = $incrementalMode['histDays'];              // datum. rozsah historie pro tvorbu reportu - pole má klíče "start" a "end", kde musí být "start" >= "end"

/* import parametru z JSON řetězce v definici Customer Science PHP v KBC:
    {
      "integrityValidationOn": true,
      "incrementalMode": {
        "incrementalOn": false,
        "incrCallsOnly": false,
        "histDays": {
          "start": 1200,
          "end":   1150
        }
      },
      "processedInstances": {
        "1": true,
        "2": true,
        "3": true,
        "4": false
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

!!!  UPOZORNĚNÍ: 
je-li zapnuto "integrityValidationOn", je nutné použít inkremenální režim s obezřetně voleným datum. rozsahem, jinak custom science skončí po 6 hodinách (dat je moc)
*/