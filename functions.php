<?php
// funkce
                                                        // prefixování hodnoty atributu identifikátorem instance + nastavení požadované délky num. řetězců
function setIdLength ($instId = 0, $str, $useInstPref = true, $objType = "tab") { 
    global $idFormat, $fakeId;
    $len = $objType=="tab" ? $idFormat["idTab"] : $idFormat["idField"]; // jde o ID položky tabuky / ID form. pole (objType = tab / fielf)
    switch ($str) {
        case "":        return "";                      // vstupní hodnota je prázdný řetězec
        case $fakeId:   return $fakeId;                 // vstupní hodnota je prázdný řetězec po průchodem fcí emptyToNA, tj. $fakeId (typicky 'n/a')
        default:        $idFormated = !empty($len) ? sprintf('%0'.$len.'s', $str) : $str;
                        switch ($useInstPref) {         // true = prefixovat hodnotu identifikátorem instance a oddělovacím znakem
                            case true:  return sprintf('%0'.$idFormat["instId"].'s', $instId) . $idFormat["sep"] . $idFormated;
                            case false: return $idFormated;    
                        }   
    }
}                                                       // prefixují se jen vyplněné hodnoty (strlen > 0)
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function logInfo ($text, $dumpLevel="basicStatusInfo") {// volitelné diagnostické výstupy do logu
    global $diagOutOptions;
    $dumpKey = array_key_exists($dumpLevel, $diagOutOptions) ? $dumpLevel : "basicStatusInfo";
    echo $diagOutOptions[$dumpKey] ? $text."\n" : "";
}
// ==============================================================================================================================================================================================
function groupNameParse ($str) {                        // separace názvu skupiny jako podřetězce ohraničeného definovanými delimitery z daného řetězce
    global $delim;
    $match = [];                                        // "match array"
    preg_match("/".preg_quote($delim["L"])."(.*?)".preg_quote($delim["R"])."/s", $str, $match);
    return empty($match[1]) ?  "" : $match[1];          // $match[1] obsahuje podřetězec ohraničený delimitery ($match[0] dtto včetně delimiterů)
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function phoneNumberCanonic ($str) {                    // veřejná tel. čísla omezená na číslice 0-9 (48-57D = 30-39H), bez úvodních nul (ltrim)
    $strConvert = ltrim(preg_replace("/[\\x00-\\x2F\\x3A-\\xFF]/", "", $str), "0");
    return (strlen($strConvert) == 9 ? "420" : "") . $strConvert;
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function trim_all ($str, $what = NULL, $thrownWith = " ", $replacedWith = "| ") {      // odebrání nadbytečných mezer a formátovacích znaků z řetězce
    if ($what === NULL) {
        //  character   dec     hexa    use
        //  "\0"         0      \\x00   Null Character
        //  "\t"         9      \\x09   Tab
        //  "\n"        10      \\x0A   New line
        //  "\x0B"      11      \\x0B   Vertical Tab
        //  "\r"        13      \\x0D   New Line in Mac
        //  " "         32      \\x20   Space       
        $charsToThrow   = "\\x00-\\x09\\x0B-\\x20\\xFF";// all white-spaces and control chars (hexa)
        $charsToReplace = "\\x0A";                      // new line
    }
    $str = preg_replace("/[".$charsToThrow . "]+/", $thrownWith,   $str);       // náhrada prázdných a řídicích znaků mezerou
    $str = preg_replace("/[".$charsToReplace."]+/", $replacedWith, $str);       // náhrada odřádkování znakem "|" (vyskytují se i vícenásobná odřádkování)
    $str = str_replace ("|  ", "", $str);                                       // odebrání mezer oddělených "|" zbylých po vícenásobném odřádkování
    $str = str_replace ("\N" , "", $str);                   // zbylé "\N" způsobují chybu importu CSV do výst. tabulek ("Missing data for not-null field")
    return $str;
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function substrInStr ($str, $substr) {                                          // test výskytu podřetězce v řetězci
    return strlen(strstr($str, $substr)) > 0;                                   // vrací true / false
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function mb_ucwords ($str) {                                                    // ucwords pro multibyte kódování
    return mb_convert_case($str, MB_CASE_TITLE, "UTF-8");
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function convertAddr ($str) {                                                   // nastavení velikosti písmen u adresy (resp. částí dresy)
    global $keywords;
    $addrArrIn  = explode(" ", $str);                                           // vstupní pole slov
    $addrArrOut = [];                                                           // výstupní pole slov
    foreach($addrArrIn as $id => $word) {                                       // iterace slov ve vstupním poli
        switch ($id) {                                                          // $id ... pořadí slova
            case 0:     $addrArrOut[] =  mb_ucwords($word); break;              // u 1. slova jen nastavit velké 1. písmeno a ostatní písmena malá
            default:    $wordLow = mb_strtolower($word, "UTF-8");               // slovo malými písmeny (pro test výskytu slova v poli $keywords aj.)
                        if (in_array($wordLow, $keywords["noConv"])) {
                            $addrArrOut[] = $word;                              // nelze rozhodnout mezi místopis. předložkou a řím. číslem → bez case konverze
                        } elseif (in_array($wordLow, $keywords["addrVal"])) {
                            $addrArrOut[] = $wordLow;                           // místopisné předložky a místopisná označení malými písmeny
                        } elseif (in_array($wordLow, $keywords["romnVal"])) {
                            $addrArrOut[] = strtoupper($word);                  // římská čísla velkými znaky
                        } else {
                            $addrArrOut[] = mb_ucwords($word);                  // 2. a další slovo, pokud není uvedeno v $keywords
                        }
        }
    }
    return implode(" ", $addrArrOut);
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function remStrMultipl ($str, $delimiter = " ") {                               // převod multiplicitních podřetězců v řetězci na jeden výskyt podřetězce
    return implode($delimiter, array_unique(explode($delimiter, $str)));
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function convertDate ($dateStr) {                                               // konverze data různého (i neznámého) formátu na požadovaný formát
    if (strlen($dateStr) <= 12) {$dateStr = str_replace(" ", "", $dateStr);}    // odebrání mezer u data do délky dd. mm. rrrr (12 znaků)
    $dateStr = preg_replace("/_/", "-", $dateStr);                              // náhrada případných podtržítek pomlčkami
    try {
        $date = new DateTime($dateStr);                                         // pokus o vytvoření objektu $date jako instance třídy DateTime z $dateStr
    } catch (Exception $e) {                                                    // $dateStr nevyhovuje konstruktoru třídy DateTime ...  
        return $dateStr;                                                        // ... vrátí původní datumový řetězec (nelze převést na požadovaný tvar)
    }                                                                           // $dateStr vyhovuje konstruktoru třídy DateTime ...  
    return $date -> format( (!strpos($dateStr, "/") ? 'Y-m-d' : 'Y-d-m') );     // ... vrátí rrrr-mm-dd (u delimiteru '/' je třeba prohodit m ↔ d)
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function convertMail ($mail) {                                                  // validace e-mailové adresy a převod na malá písmena
    $mail = strtolower($mail);                                                  // převod e-mailové adresy na malá písmena
    $isValid = !(!filter_var($mail, FILTER_VALIDATE_EMAIL));                    // validace e-mailové adresy
    return $isValid ? $mail : "(nevalidní e-mail) ".$mail;                      // vrátí buď e-mail (lowercase), nebo e-mail s prefixem "(nevalidní e-mail) "
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function convertPSC ($str) {                                                    // vrátí buď PSČ ve tvaru xxx xx (validní), nebo "nevalidní PSČ ve formuláři"
    $str = str_replace(" ", "", $str);                                          // odebrání mezer => pracovní tvar validního PSČ je xxxxx
    return (is_numeric($str) && strlen($str) == 5) ? substr($str, 0, 3)." ".substr($str, 3, 2) : "nevalidní PSČ ve formuláři";  // finální tvar PSČ je xxx xx
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function convertFieldValue ($idfield, $val) {                                   // validace + případná korekce hodnot formulářových polí
    global $fields, $keywords;                                                  // $key = název klíče form. pole; $val = hodnota form. pole určená k validaci
    $titleLow = mb_strtolower($fields[$idfield]["title"], "UTF-8");             // title malými písmeny (jen pro test výskytu klíčových slov v title)                                                                             
    if (in_array($titleLow, $keywords["dateEq"])) {return convertDate($val);}
    if (in_array($titleLow, $keywords["mailEq"])) {return convertMail($val);} 
    foreach (["date","name","addr","psc"] as $valType) {
        foreach ($keywords[$valType] as $substr) {
            switch ($valType) {
                case "date":    if (substrInStr($titleLow, $substr)) {return convertDate($val);}    continue;
                case "name":    if (substrInStr($titleLow, $substr)) {return mb_ucwords($val) ;}    continue;
                case "addr":    if (substrInStr($titleLow, $substr)) {return convertAddr($val);}    continue;
                case "psc" :    if (substrInStr($titleLow, $substr)) {return convertPSC($val) ;}    continue;
            }
        }
    }
    return $val;        // hodnota nepodléhající validaci a korekci (žádná část title form. pole není v $keywords[$valType]
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function boolValsUnify ($val) {         // dvojici booleovských hodnot ("",1) u v6 převede na dvojici hodnot (0,1) používanou u v5 (lze použít u booleovských atributů)
    global $inst;
    switch ($inst["ver"]) {
        case 5: return $val;                    // v5 - hodnoty 0, 1 → propíší se
        case 6: return $val=="1" ? $val : "0";  // v6 - hodnoty "",1 → protože jde o booleovskou proměnnou, nahradí se "" nulami
    }
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function actionCodeToName ($actCode) {  // atribut "action" typu ENUM - převod číselného kódu akce na název akce
    global $campRecordsActions;
    return array_key_exists($actCode, $campRecordsActions) ? $campRecordsActions[$actCode] : $actCode;
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function setFieldsShift () {            // posun v ID formCrmFields oproti ID formFields
    global $formFieldsIdShift, $formCrmFieldsIdShift, $idFormat;
    $formFieldsIdShift    = 0;
    $formCrmFieldsIdShift = pow(10, $idFormat["idTab"] - 1);   // přidá v indexu číslici 1 na první pozici zleva (číslice nejvyššího řádu)
}
// ==============================================================================================================================================================================================
function initGroups () {                // nastavení výchozích hodnot proměnných popisujících skupiny
    global $groups, $idGroup, $tabItems;
    $groups             = [];           // 1D-pole skupin - prvek pole má tvar groupName => idgroup
    $idGroup            = 0;            // umělý inkrementální index pro číslování skupin
    $tabItems["groups"] = 0;            // vynulování počitadla záznamů v tabulce 'groups'
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function initStatuses () {              // nastavení výchozích hodnot proměnných popisujících stavy
    global $statuses, $idStatus, $idstatusFormated, $tabItems;
    $statuses = [];                     /* 3D-pole stavů - prvek pole má tvar  <statusId> => ["title" => <hodnota>, "statusIdOrig" => [pole hodnot]],
                                           kde statusId a title jsou unikátní, statusId jsou neformátované indexy (bez prefixu instance, který v commonStatus
                                           režimu nemá význam, a bez formátování na počet číslic požadovaný ve výstupních tabulkách)
                                           a v poli statusIdOrig jsou originální (prefixované) ID stejnojmenných stavů z různých instancí  */
    $idStatus             = 0;          // umělý inkrementální index pro číslování stavů (1, 2, ...)
    $tabItems["statuses"] = 0;          // vynulování počitadla záznamů v tabulce 'statuses'
    unset($idstatusFormated);           // formátovaný umělý index stavu ($idStatus doplněný na počet číslic požadovaný ve výstupních tabulkách)
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function initFields () {                // nastavení výchozích hodnot proměnných popisujících formulářová pole
    global $fields;
    $fields = [];                       // 2D-pole formulářových polí - prvek pole má tvar <name> => ["idfield" => <hodnota>, "title" => <hodnota>]    
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function iterStatuses ($val, $valType = "statusIdOrig") {               // prohledání 3D-pole stavů $statuses
    global $statuses, $emptyToNA, $fakeId;                              // $val = hledaná hodnota;  $valType = "title" / "statusIdOrig"
    if ($valType=="statusIdOrig" && $emptyToNA && $val==$fakeId) {
        return $fakeId;                                                 // původně prázdná hodnota FK je nahrazena hodnotou $fakeId (typicky "n/a")
    }
    foreach ($statuses as $statId => $statRow) {
        switch ($valType) {
            case "title":           // $statRow[$valType] je string
                                    if ($statRow[$valType] == $val) {   // zadaná hodnota v poli $statuses nalezena
                                        return $statId;                 // ... → vrátí id (umělé) položky pole $statuses, v níž se hodnota nachází
                                    }
                                    break;
            case "statusIdOrig":    // $statRow[$valType] je 1D-pole
                                    foreach ($statRow[$valType] as $statVal) {
                                        if ($statVal == $val) {         // zadaná hodnota v poli $statuses nalezena
                                            return $statId;             // ... → vrátí id (umělé) položky pole $statuses, v níž se hodnota nachází
                                        }
                                    }
        }        
    }
    return false;                   // zadaná hodnota v poli $statuses nenalezena
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function dateRngCheck ($val, $type = "tabRow") {                        // $val je datumočas ze zkoumaného záznamu
    global $incrementalOn, $processedDates, $pkValsProcessedDates;
    switch ($type) {
        case "tabRow":  $dateRng = $processedDates; break;              // pro validaci datumového rozsahu záznamů při zpracovávání záznamů
        case "pkVals":  $dateRng = $pkValsProcessedDates;               // pro validaci datumového rozsahu záznamů při sbírání hodnot PK do pole $pkVals
    }
    $rowDate   = substr($val, 0, 10);                                   // datum ze zkoumaného záznamu
    return ($incrementalOn && ($rowDate < $dateRng["start"] || $rowDate > $dateRng["end"])) ? false : true;
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function emptyToNA ($id) {          // prázdné hodnoty nahradí hodnotou $fakeId - kvůli GoodData, aby zde byla nabídka $fakeTitle [volitelné]
    global $emptyToNA, $fakeId;
    return ($emptyToNA && empty($id)) ? $fakeId : $id;
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function checkIdLengthOverflow ($val) {     // kontrola, zda došlo (true) nebo nedošlo (false) k přetečení délky ID určené proměnnou $idFormat["idTab"] ...
    global $idFormat, $tab;                 // ... nebo umělým ID (groups, statuses, fieldValues)
        if ($val >= pow(10, $idFormat["idTab"])) {
            logInfo("PŘETEČENÍ DÉLKY INDEXŮ ZÁZNAMŮ V TABULCE ".$tab);          // volitelný diagnostický výstup do logu                
            $idFormat["idTab"]++;
            $digitsNo = $idFormat["instId"] + $idFormat["idTab"];
            logInfo("DÉLKA INDEXŮ NAVÝŠENA NA ".$digitsNo." ČÍSLIC");
            return true;                    // došlo k přetečení → je třeba začít plnit OUT tabulky znovu, s delšími ID
        }
    return false;                           // nedošlo k přetečení (OK)
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getJsonItem ($str, $key) {               // získání konkrétního prvku z JSON řetězce
    $decod = json_decode($str, true, JSON_UNESCAPED_UNICODE);                   // dekódovaný řetězec (je-li $str JSON, je $decod ARRAY)
    if (is_null  ($decod)) {return [];}
    if ( is_array($decod)) {
        if (array_key_exists($key, $decod)) {
            return $decod[$key];                                                // $decod[$key] je STRING, INT, BOOL nebo ARRAY (dle struktury JSONu), nejčastěji ARRAY
        }
    } else return $decod;
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function jsonParse ($formArr) {             // formArr je 2D-pole    
    global $formFieldsOuts, $tab, $fields, $idFieldSrcRec, $idFormat, $instId, $adhocDump;
    global ${"out_".$formFieldsOuts[$tab]};                                     // název out-only tabulky pro zápis hodnot formulářových polí
    foreach ($formArr as $key => $valArr) {                                     // $valArr je 1D-pole, obvykle má jen klíč 0 (nebo žádný)                                                                                                
        if (empty($valArr)) {continue;}                                         // nevyplněné formulářové pole - neobsahuje žádný prvek
        $idVal = 0;                                                             // ID hodnoty konkrétního form. pole
        foreach ($valArr as $val) {                                             // klíč = 0,1,... (nezajímavé); $val jsou hodnoty form. polí
            $fieldVals = [];                                                    // záznam do out-only tabulky 'fieldValues'
            // optimalizace hodnot formulářových polí, vyřazení prázdných hodnot
            $val = remStrMultipl($val);                                         // value (hodnota form. pole zbavená multiplicitního výskytu podřetězců)
            $val = trim_all($val);                                              // value (hodnota form. pole zbavená nadbyteč. mezer a formátovacích znaků)                                                        
            if (!strlen($val)) {continue;}                                      // prázdná hodnota prvku formulářového pole - kontrola před korekcemi                                                                                   
            // ----------------------------------------------------------------------------------------------------------------------------------
            // validace a korekce hodnoty formulářového pole + konstrukce řádku out-only tabulky 'fieldValues'
            $idVal++;                                                           // inkrement umělého ID hodnot formulářových polí
            if ($idVal == pow(10, $idFormat["idField"])) {                      // došlo k přetečení délky indexů hodnot form. polí
            logInfo("PŘETEČENÍ DÉLKY INDEXU HODNOT FORM. POLÍ V TABULCE ".$tab);// volitelný diagnostický výstup do logu
            $idFormat["idField"]++;            
            }   // výstupy se nezačínají plnit znovu od začátku, jen se navýší počet číslic ID hodnot form. polí od dotčeného místa dále
            // ----------------------------------------------------------------------------------------------------------------------------------         
            $idfield = "";
            foreach ($fields as $idfi => $field) {                              // v poli $fields dohledám 'idfield' ke známému 'name'
                $instDig       = floor($idfi/pow(10, $idFormat["idTab"]));         // číslice vyjadřující ID aktuálně zpracovávané instance
                $fieldShiftDig = floor($idfi/pow(10, $idFormat["idTab"]-1)) - 10* $instId;  // číslice vyjadřující posun indexace crmFields vůči fields (0/1) 
                if ($instDig != $instId) {continue;}                            // nejedná se o formulářové pole z aktuálně zpracovávané instance
                if (($tab == "crmRecords" && $fieldShiftDig == 0) ||
                    ($tab != "crmRecords" && $fieldShiftDig == 1) ) {continue;} // výběr form. polí odpovídajícího původu (crmFields/fields) pro daný typ tabulky
                if ($field["name"] == $key) {
                    logInfo($tab." - NALEZENO PREFEROVANÉ FORM. POLE [".$idfi.", ".$field['name'].", ".$field['title']."]", "jsonParseInfo");
                    $idfield = $idfi; break;
                }
            }
            if ($idfield == "") {   // nebylo-li nalezeno form. pole odpovídajícího name, pokračuje hledání v druhém z typů form. polí (fields/crmFields)
                logInfo($tab." - NENALEZENO PREFEROVANÉ FORM. POLE -> ", "jsonParseInfo");  // diag. výstup do logu
                foreach ($fields as $idfi => $field) {
                    $instDig       = floor($idfi/pow(10, $idFormat["idTab"]));  // číslice vyjadřující ID aktuálně zpracovávané instance
                    $fieldShiftDig = floor($idfi/pow(10, $idFormat["idTab"]-1)) - 10* $instId; // číslice vyjadřující posun indexace crmFields vůči fields (0/1)
                    if ($instDig != $instId) {continue;}                        // nejedná se o formulářové pole z aktuálně zpracovávané instance
                    if (($tab == "crmRecords" && $fieldShiftDig == 1) ||
                        ($tab != "crmRecords" && $fieldShiftDig == 0) ) {continue;} // výběr form. polí odpovídajícího původu
                    if ($field["name"] == $key) {
                        logInfo("  ALTERNATIVNÍ POLE JE [".$idfi.", ".$field['name'].", ".$field['title']."]", "jsonParseInfo");
                        $idfield = $idfi; break;
                    }
                }
            } // --------------------------------------------------------------------------------------------------------------------------------                                                              
            $val = convertFieldValue($idfield, $val);                           // je-li část názvu klíče $key v klíčových slovech $keywords, ...
                                                                                // ... vrátí validovanou/konvertovanou hodnotu $val, jinak nezměněnou $val                                                            
            if (!strlen($val)) {continue;}                                      // prázdná hodnota prvku formulářového pole - kontrola po korekcích
            $fieldVals = [
                $idFieldSrcRec . $idfield . setIdLength(0,$idVal,false,"field"),// ID cílového záznamu do out-only tabulky hodnot formulářových polí
                $idFieldSrcRec,                                                 // ID zdrojového záznamu z tabulky obsahující parsovaný JSON
                $idfield,                                                       // idfield
                $val                                                            // korigovaná hodnota formulářového pole
            ];                                                                                                                                                                     
            ${"out_".$formFieldsOuts[$tab]} -> writeRow($fieldVals);            // zápis řádku do out-only tabulky hodnot formulářových polí
            if ($adhocDump["active"]) {if ($adhocDump["idFieldSrcRec"] == $idFieldSrcRec) {
                echo $tab." - ADHOC DUMP (\$key = ".$key."): [idVal ".$fieldVals[0].", idSrcRec ".$fieldVals[1].", idfield ".$fieldVals[2].", val ".$fieldVals[3]."]\n";}}
        }    
    }
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function jsonProcessing ($instId, $tab, $colName, $hodnota) {
    global $jsonList;
    if (array_key_exists($instId, $jsonList)) {
        if (array_key_exists($tab, $jsonList[$instId])) {
            if (array_key_exists($colName, $jsonList[$instId][$tab])) {         // sloupec obsahuje JSON
                $formArr = json_decode($hodnota, true, JSON_UNESCAPED_UNICODE);
                if (!is_null($formArr)) {jsonParse($formArr);}                  // hodnota dekódovaného JSONu není NULL → lze ji prohledávat jako pole                
                return $jsonList[$instId][$tab][$colName] ? true : false;       // buňka obsahovala JSON; po návratu z fce pokračovat/nepokračovat ve zpracování hodnoty
            }
        }
    }
    return true;                                                                // buňka neobsahovala JSON, po návratu z fce pokračovat ve zpracování hodnoty
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function dateRestrictColId ($instId, $tab) {                                          // nalezení ID sloupce pro datumovou restrikci záznamů (pokud takový sloupec v tabulce existuje)
    global $tiList;
    if (array_key_exists($instId, $tiList)){
        if (array_key_exists($tab, $tiList[$instId])) {                         // pro danou kombinaci instance-tabulka existuje atribut pro datumovou restrikci (není to např. statický údaj)
            return $tiList[$instId][$tab];                                      // ID sloupce, který je v dané tabulce atributem pro datumovou restrikci (0, 1, 2, ...)
        }
    }                                                                           // při nenalezení ID sloupce pro datumovou restrikci (např. u statických tabulek) vrátí NULL
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function colParentTab ($instId, $tab, $colName) {                               // nalezení názvu nadřazené tabulky pro daný sloupec (je-li sloupec FK)
    global $fkList;     //echo " | count(\$fkList) = ".count($fkList);
    if (array_key_exists($instId, $fkList)) {
        if (array_key_exists($tab, $fkList[$instId])) {
            if (array_key_exists($colName, $fkList[$instId][$tab])) {   //echo " | colParentTab-returns ".$fkList[$instId][$tab][$colName];
                return $fkList[$instId][$tab][$colName];                        // daný sloupec je FK → vrátí název nadřazené tabulky
            }
        }
    }                               // echo " | colParentTab-returnsNULL";      // daný sloupec není FK → vrátí NULL
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function integrityValid ($instId, $tab, $colName, $unprefixVal) {               // integritní validace
    global $pkVals, $emptyToNA;     //echo " | count(\$pkVals) = ".count($pkVals);
    $colParentTab = colParentTab($instId, $tab, $colName);                      // název nadřazené tabulky u sloupce, který je FK
                                    //echo is_null($colParentTab) ? "" : " | \$colParentTab(".$instId.", ".$tab.", ".$colName.") = ".$colParentTab." | ";
    if (is_null($colParentTab)) {return "notFK";}                               // daný sloupec není FK → vrátí "notFK"
    if (empty($unprefixVal) && $emptyToNA) {return "2fakeFK";}                  // hodnota FK je sice prázdná, ale bude nahrazena hodnotou $fakeId (typicky "n/a")
    if (array_key_exists($instId, $pkVals)) {                                   // test existance odpovídajícího záznamu v nadřazené tabulce
        if (array_key_exists($colParentTab, $pkVals[$instId])) {
            if (in_array($unprefixVal, $pkVals[$instId][$colParentTab])) {
                return "validFK";                                               // hodnota $unprefixVal byla nalezena v hodnotách PK nadřazené tabulky
            } else {
                logInfo("HODNOTA ".$instId."_".$tab.".".$colName." = \"".$unprefixVal."\" NEMÁ NADŘAZENÝ ZÁZNAM V TABULCE ".$colParentTab." -> NEBUDE PROPSÁNA NA VÝSTUP", "detailIntegrInfo");
                return "wrongFK";                                               // hodnota $unprefixVal nebyla nalezena v hodnotách PK nadřazené tabulky
            }
        }
    }
    return "unfound";                                                           // v poli $pkVals nenalezen některý z potřebných klíčů → o integritní správnosti nelze rozhodnout (vrátí "NA")
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function tabItemsIncr ($colName, $integrValidResult) {                          // inkrement počitadel vstupních záznamů všech/vyhovujících/nevyhovujících integritní validaci
    global $integrValidCounts;                                                  // $integrValidResult = "integrOk" / "integrFak" / "integrErr"
    // test existence potřebných počitadel v poli $integrValidCounts, založení chybějících počitadel s nulovou hodnotou
    if(!array_key_exists($colName, $integrValidCounts))              {$integrValidCounts[$colName] = [];}
    if(!array_key_exists("total" , $integrValidCounts))              {$integrValidCounts["total"]  = [];}
    if(!array_key_exists("integrOk" , $integrValidCounts[$colName])) {$integrValidCounts[$colName]["integrOk"] = 0;}  
    if(!array_key_exists("integrFak", $integrValidCounts[$colName])) {$integrValidCounts[$colName]["integrFak"]= 0;}
    if(!array_key_exists("integrErr", $integrValidCounts[$colName])) {$integrValidCounts[$colName]["integrErr"]= 0;}
    if(!array_key_exists("integrOk" , $integrValidCounts["total"] )) {$integrValidCounts["total"] ["integrOk"] = 0;}
    if(!array_key_exists("integrFak", $integrValidCounts["total"] )) {$integrValidCounts["total"] ["integrFak"]= 0;}
    if(!array_key_exists("integrErr", $integrValidCounts["total"] )) {$integrValidCounts["total"] ["integrErr"]= 0;}
    // inkrement požadovaných počitadel
    $integrValidCounts[$colName][$integrValidResult]++;
    $integrValidCounts["total"][$integrValidResult]++;
}