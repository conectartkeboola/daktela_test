<?php
// TRANSFORMACE DAT Z LIBOVOLNÉHO POČTU INSTANCÍ DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// parametry importované z konfiguračního JSON v KBC
$callsIncrementalOutput = $config["parameters"]["callsIncrementalOutput"];
$diagOutOptions         = $config["parameters"]["diagOutOptions"];          // diag. výstup do logu Jobs v KBC - klíče: basicStatusInfo, jsonParseInfo, detailIntegrInfo
$adhocDump              = $config["parameters"]["adhocDump"];               // diag. výstup do logu Jobs v KBC - klíče: active, idFieldSrcRec

// full load / incremental load výstupní tabulky 'calls'
$incrementalOn = !empty($callsIncrementalOutput['incrementalOn']) ? true : false;   // vstupní hodnota false se vyhodnotí jako empty :)

// za jak dlouhou historii [dny] se generuje inkrementální výstup (0 = jen za aktuální den, 1 = od včerejšího dne včetně [default], ...)
$jsonHistDays   = $callsIncrementalOutput['incremHistDays'];
$incremHistDays = $incrementalOn && !empty($jsonHistDays) && is_numeric($jsonHistDays) ? $jsonHistDays : 1;
/* import parametru z JSON řetězce v definici Customer Science PHP v KBC:
    {
        "callsIncrementalOutput": {
            "incrementalOn": true,
            "incremHistDays": 3
        },
        "diagOutOptions": {
            "basicStatusInfo": true
        }
    }
  -> podrobnosti viz https://developers.keboola.com/extend/custom-science
*/
// ==============================================================================================================================================================================================
// proměnné a konstanty

// seznam instancí Daktela
$instances = [  1   =>  ["url" => "https://ilinky.daktela.com",     "ver" => 5],
                2   =>  ["url" => "https://dircom.daktela.com",     "ver" => 5],
                3   =>  ["url" => "https://conectart.daktela.com",  "ver" => 6]
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// struktura tabulek

// vstupně-výstupní tabulky (načtou se jako vstupy, transformují se a výsledek je zapsán jako výstup)
$tabsInOutV5  = [
// "tab" => ["instPrf" - prefixovat hodnoty ve sloupci identifikátorem instance (0/1), "pk" - primární klíč (0/1), "fk" - cizí klíč (tabName)]
    "calls"             =>  [   "idcall"                =>  ["instPrf" => 1, "pk" => 1],
                                "call_time"             =>  ["instPrf" => 0],
                                "direction"             =>  ["instPrf" => 0],
                                "answered"              =>  ["instPrf" => 0],
                                "idqueue"               =>  ["instPrf" => 1, "fk" => "queues"],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"],
                                "clid"                  =>  ["instPrf" => 0],
                                "contact"               =>  ["instPrf" => 0],
                                "did"                   =>  ["instPrf" => 0],
                                "wait_time"             =>  ["instPrf" => 0],
                                "ringing_time"          =>  ["instPrf" => 0],
                                "hold_time"             =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "orig_pos"              =>  ["instPrf" => 0],
                                "position"              =>  ["instPrf" => 0],
                                "disposition_cause"     =>  ["instPrf" => 0],
                                "disconnection_cause"   =>  ["instPrf" => 0],
                                "pressed_key"           =>  ["instPrf" => 0],
                                "missed_call"           =>  ["instPrf" => 0],
                                "missed_call_time"      =>  ["instPrf" => 0],
                                "score"                 =>  ["instPrf" => 0],
                                "note"                  =>  ["instPrf" => 0],
                                "attemps"               =>  ["instPrf" => 0],
                                "qa_user_id"            =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0, "fk" => "instances"]
                            ]
];
$tabsInOutV56 = [            
    "loginSessions"     =>  [   "idloginsession"        =>  ["instPrf" => 1, "pk" => 1],
                                "start_time"            =>  ["instPrf" => 0],
                                "end_time"              =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"]
                            ],
    "pauseSessions"     =>  [   "idpausesession"        =>  ["instPrf" => 1, "pk" => 1],
                                "start_time"            =>  ["instPrf" => 0],
                                "end_time"              =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "idpause"               =>  ["instPrf" => 0, "fk" => "pauses"],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"]
                            ],
    "queueSessions"     =>  [   "idqueuesession"        =>  ["instPrf" => 1, "pk" => 1],
                                "start_time"            =>  ["instPrf" => 0], 
                                "end_time"              =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "idqueue"               =>  ["instPrf" => 1, "fk" => "queues"],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"]
                            ],
    "users"             =>  [   "iduser"                =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0, "fk" => "instances"],
                                "email"                 =>  ["instPrf" => 0]
                            ],
    "pauses"            =>  [   "idpause"               =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],
                                "type"                  =>  ["instPrf" => 0],
                                "paid"                  =>  ["instPrf" => 0]
                            ],
    "queues"            =>  [   "idqueue"               =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0, "fk" => "instances"],
                                "idgroup"               =>  ["instPrf" => 1, "fk" => "groups"]
                            ],                              // 'idgroup' je v IN tabulce NÁZEV → neprefixovat
    "statuses"          =>  [   "idstatus"              =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0]
                            ],
    "recordSnapshots"   =>  [   "idrecordsnapshot"      =>  ["instPrf" => 1, "pk" => 1],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"],
                                "idrecord"              =>  ["instPrf" => 1, "fk" => "records"],
                                "idstatus"              =>  ["instPrf" => 1, "fk" => "statuses"],
                                "idcall"                =>  ["instPrf" => 1, "fk" => "calls"],
                                "created"               =>  ["instPrf" => 0],
                                "created_by"            =>  ["instPrf" => 1],
                                "nextcall"              =>  ["instPrf" => 0]
                            ],
    "fields"            =>  [   "idfield"               =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0, "fk" => "instances"],
                                "name"                  =>  ["instPrf" => 0]
                            ],
    "records"           =>  [   "idrecord"              =>  ["instPrf" => 1, "pk" => 1],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"],
                                "idqueue"               =>  ["instPrf" => 1, "fk" => "queues"],
                                "idstatus"              =>  ["instPrf" => 1, "fk" => "statuses"],
                                "iddatabase"            =>  ["instPrf" => 1, "fk" => "databases"],
                                "number"                =>  ["instPrf" => 0],
                                "idcall"                =>  ["instPrf" => 1, "fk" => "calls"],
                                "action"                =>  ["instPrf" => 0],
                                "edited"                =>  ["instPrf" => 0],
                                "created"               =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],
                                "form"                  =>  ["instPrf" => 0]
                            ]
];
// nutno dodržet pořadí tabulek:
// - 'records' a 'recordSnapshots' se odkazují na 'statuses'.'idstatus' → musí být uvedeny až za 'statuses' (pro případ použití commonStatuses)
// - 'records' a 'fieldValues' se tvoří pomocí pole $fields vzniklého z tabulky 'fields' → musí být uvedeny až za 'fields' (kvůli foreach)
$tabsInOutV6 = [            // vstupně-výstupní tabulky používané pouze u Daktely v6
    "databases"         =>  [   "iddatabase"            => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "idqueue"               => ["instPrf" => 1, "fk" => "queues"],
                                "description"           => ["instPrf" => 0],
                                "stage"                 => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "time"                  => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "contacts"          =>  [   "idcontact"             => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "firstname"             => ["instPrf" => 0],
                                "lastname"              => ["instPrf" => 0],
                                "idaccount"             => ["instPrf" => 1, "fk" => "accounts"],
                                "iduser"                => ["instPrf" => 1, "fk" => "users"],
                                "description"           => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0],
                                "form"                  => ["instPrf" => 0],
                                "number"                => ["instPrf" => 0]
                            ],
    "ticketSla"         =>  [   "idticketsla"           => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "response_low"          => ["instPrf" => 0],
                                "response_normal"       => ["instPrf" => 0],
                                "response_high"         => ["instPrf" => 0],
                                "solution_low"          => ["instPrf" => 0],
                                "solution_normal"       => ["instPrf" => 0],
                                "solution_high"         => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "accounts"          =>  [   "idaccount"             => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "idticketsla"           => ["instPrf" => 1, "fk" => "ticketSla"],
                                "survey"                => ["instPrf" => 0],
                                "iduser"                => ["instPrf" => 1, "fk" => "users"],
                                "description"           => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "ticketCategories"  =>  [   "idticketcategory"      => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "idticketsla"           => ["instPrf" => 1, "fk" => "ticketSla"],
                                "idqueue"               => ["instPrf" => 1, "fk" => "queues"],
                                "survey"                => ["instPrf" => 0],
                                "template_email"        => ["instPrf" => 0],
                                "template_page"         => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "tickets"           =>  [   "idticket"              => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "idticketcategory"      => ["instPrf" => 1, "fk" => "ticketCategories"],
                                "iduser"                => ["instPrf" => 1, "fk" => "users"],
                                "email"                 => ["instPrf" => 0],
                                "idcontact"             => ["instPrf" => 1, "fk" => "contacts"],
                                "idstatus"              => ["instPrf" => 1, "fk" => "statuses"],
                                "description"           => ["instPrf" => 0],
                                "stage"                 => ["instPrf" => 0],
                                "priority"              => ["instPrf" => 0],
                                "sla_deadtime"          => ["instPrf" => 0],
                                "sla_change"            => ["instPrf" => 0],
                                "sla_notify"            => ["instPrf" => 0],
                                "sla_duration"          => ["instPrf" => 0],
                                "sla_custom"            => ["instPrf" => 0],
                                "survey"                => ["instPrf" => 0],
                                "survey_offered"        => ["instPrf" => 0],
                                "satisfaction"          => ["instPrf" => 0],
                                "satisfaction_comment"  => ["instPrf" => 0],
                                "reopen"                => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "created"               => ["instPrf" => 0],
                                "edited"                => ["instPrf" => 0],
                                "edited_by"             => ["instPrf" => 1],
                                "first_answer"          => ["instPrf" => 0],
                                "first_answer_duration" => ["instPrf" => 0],
                                "closed"                => ["instPrf" => 0],
                                "unread"                => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0],
                                "form"                  => ["instPrf" => 0]
                            ],
    "crmRecordTypes"    =>  [   "idcrmrecordtype"       => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "description"           => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "created"               => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "crmRecords"        =>  [   "idcrmrecord"           => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],    
                                "title"                 => ["instPrf" => 0],
                                "idcrmrecordtype"       => ["instPrf" => 1, "fk" => "crmRecordTypes"],
                                "iduser"                => ["instPrf" => 1, "fk" => "users"],
                                "idcontact"             => ["instPrf" => 1, "fk" => "contacts"],
                                "idaccount"             => ["instPrf" => 1, "fk" => "accounts"],
                                "idticket"              => ["instPrf" => 1, "fk" => "idticket"],
                                "idstatus"              => ["instPrf" => 1, "fk" => "idstatus"],
                                "description"           => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "edited"                => ["instPrf" => 0],
                                "created"               => ["instPrf" => 0],
                                "stage"                 => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0],
                                "form"                  => ["instPrf" => 0]
                            ],
    "crmRecordSnapshots"=>  [   "idcrmrecordsnapshot"   => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "idcontact"             => ["instPrf" => 1, "fk" => "contacts"],
                                "idaccount"             => ["instPrf" => 1, "fk" => "accounts"],
                                "idticket"              => ["instPrf" => 1, "fk" => "tickets"],
                                "idcrmrecord"           => ["instPrf" => 1, "fk" => "crmRecords"],
                                "iduser"                => ["instPrf" => 1, "fk" => "users"],
                                "idstatus"              => ["instPrf" => 1, "fk" => "statuses"],
                                "idcrmrecordtype"       => ["instPrf" => 1, "fk" => "crmRecordTypes"],
                                "description"           => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "created_by"            => ["instPrf" => 0],
                                "time"                  => ["instPrf" => 0],
                                "stage"                 => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "activities"        =>  [   "idactivity"            => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "idcontact"             => ["instPrf" => 1, "fk" => "contacts"],
                                "idticket"              => ["instPrf" => 1, "fk" => "tickets"],
                                "idqueue"               => ["instPrf" => 1, "fk" => "queues"],
                                "iduser"                => ["instPrf" => 1, "fk" => "users"],
                                "idrecord"              => ["instPrf" => 1, "fk" => "records"],
                                "idstatus"              => ["instPrf" => 1, "fk" => "statuses"],
                                "action"                => ["instPrf" => 0],
                                "type"                  => ["instPrf" => 0],
                                "priority"              => ["instPrf" => 0],
                                "description"           => ["instPrf" => 0],
                                "time"                  => ["instPrf" => 0],
                                "time_wait"             => ["instPrf" => 0],
                                "time_open"             => ["instPrf" => 0],
                                "time_close"            => ["instPrf" => 0],
                                "created_by"            => ["instPrf" => 1],
                                "idinstance"            => ["instPrf" => 0],
                                "item"                  => ["instPrf" => 0]
                            ]
];
$tabsInOut = [
    5                   =>  array_merge($tabsInOutV5, $tabsInOutV56),
    6                   =>  array_merge($tabsInOutV56, $tabsInOutV6)
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// jen výstupní tabulky
$tabsOutOnlyV56 = [         // tabulky, které vytváří transformace a objevují se až na výstupu (nejsou ve vstupním bucketu KBC) používané u Daktely v5 i v6
    "fieldValues"       =>  [   "idfieldvalue"          => ["instPrf" => 1],
                                "idrecord"              => ["instPrf" => 1],
                                "idfield"               => ["instPrf" => 1],
                                "value"                 => ["instPrf" => 0]
                            ],
    "groups"            =>  [   "idgroup"               => ["instPrf" => 1],
                                "title"                 => ["instPrf" => 0]
                            ],
    "instances"         =>  [   "idinstance"            => ["instPrf" => 0],
                                "url"                   => ["instPrf" => 0]
                            ]
];
$tabsOutOnlyV6 = [          // tabulky, které vytváří transformace a objevují se až na výstupu (nejsou ve vstupním bucketu KBC) používané pouze u Daktely v6
    "calls"             =>  [   "idcall"                => ["instPrf" => 1],
                                "call_time"             => ["instPrf" => 0],
                                "direction"             => ["instPrf" => 0],
                                "answered"              => ["instPrf" => 0],
                                "idqueue"               => ["instPrf" => 1],
                                "iduser"                => ["instPrf" => 1],
                                "clid"                  => ["instPrf" => 0],
                                "contact"               => ["instPrf" => 0],
                                "did"                   => ["instPrf" => 0],
                                "wait_time"             => ["instPrf" => 0],
                                "ringing_time"          => ["instPrf" => 0],
                                "hold_time"             => ["instPrf" => 0],
                                "duration"              => ["instPrf" => 0],
                                "orig_pos"              => ["instPrf" => 0],
                                "position"              => ["instPrf" => 0],
                                "disposition_cause"     => ["instPrf" => 0],
                                "disconnection_cause"   => ["instPrf" => 0],
                                "pressed_key"           => ["instPrf" => 0],
                                "missed_call"           => ["instPrf" => 0],
                                "missed_call_time"      => ["instPrf" => 0],
                                "score"                 => ["instPrf" => 0],
                                "note"                  => ["instPrf" => 0],
                                "attemps"               => ["instPrf" => 0],
                                "qa_user_id"            => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    "contFieldVals"     =>  [   "idcontfieldval"        => ["instPrf" => 1],
                                "idcontact"             => ["instPrf" => 1],
                                "idfield"               => ["instPrf" => 1],
                                "value"                 => ["instPrf" => 0]
                            ],                                                  // hodnoty formulářových polí z tabulky "contacts"
    "tickFieldVals"     =>  [   "idtickfieldval"        => ["instPrf" => 1],
                                "idticket"              => ["instPrf" => 1],
                                "idfield"               => ["instPrf" => 1],
                                "value"                 => ["instPrf" => 0]
                            ],                                                  // hodnoty formulářových polí z tabulky "tickets"
    "crmFieldVals"      =>  [   "idcrmfieldval"         => ["instPrf" => 1],
                                "idcrmrecord"           => ["instPrf" => 1],
                                "idfield"               => ["instPrf" => 1],
                                "value"                 => ["instPrf" => 0]
                            ],                                                  // hodnoty formulářových polí z tabulky "crmRecords"
    /* "actItemVals"    =>  [   "idactfieldval"         => ["instPrf" => 1],
                                "idactivity"            => ["instPrf" => 1],
                                "idfield"               => ["instPrf" => 1],
                                "value"                 => ["instPrf" => 0]
                            ]                                                   // hodnoty pole "item" z tabulky "contacts" 
    */
];
$tabsOutOnly = [
    5                   =>  $tabsOutOnlyV56,
    6                   =>  array_merge($tabsOutOnlyV56, $tabsOutOnlyV6)
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// parametry parsování JSON řetězců záznamů z formulářových polí do out-only tabulek hodnot formulářových polí
$formFieldsOuts = [     // <vstupní tabulka kde se nachází form. pole> => [<název out-only tabulky hodnot form. polí>, <umělý inkrementální index hodnot form. polí>]
    "records"       =>  "fieldValues",
    "contacts"      =>  "contFieldVals",
    "tickets"       =>  "tickFieldVals",
    "crmRecords"    =>  "crmFieldVals",
    //"activities"  =>  "actItemVals"
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// jen vstupní tabulky
$tabsInOnlyV5  = $tabsInOnlyV56 = [];
$tabsInOnlyV6  = [
    "crmFields"         =>  [   "idcrmfield"            => ["instPrf" => 1],
                                "title"                 => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0],
                                "name"                  => ["instPrf" => 0]
                            ]
];
$tabsInOnly = [
    5                   =>  array_merge($tabsInOnlyV5, $tabsInOnlyV56),
    6                   =>  array_merge($tabsInOnlyV56, $tabsInOnlyV6)
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// jen vstupní sloupce
$colsInOnly = [         // seznam sloupců, které se nepropíší do výstupních tabulek (slouží jen k internímu zpracování)
 // "název_tabulky"     =>  ["název_sloupce_1", "název_sloupce_2, ...]
    "fields"            =>  ["name"],   // systémové názvy formulářových polí, slouží jen ke spárování "čitelných" názvů polí s hodnotami polí parsovanými z JSONu
    "records"           =>  ["form"],   // hodnoty formulářových polí z tabulky "records"    jako neparsovaný JSON
    "contacts"          =>  ["form"],   // hodnoty formulářových polí z tabulky "contacts"   jako neparsovaný JSON
    "tickets"           =>  ["form"],   // hodnoty formulářových polí z tabulky "tickets"    jako neparsovaný JSON
    "crmRecords"        =>  ["form"],   // hodnoty formulářových polí z tabulky "crmRecords" jako neparsovaný JSON
    //"activities"      =>  [...]
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// proměnné pro práci se všemi tabulkami
$tabs_InOut_InOnly = [     // nutno dodržet pořadí spojování polí, aby in-only tabulka crmFields (v6) byla před tabulkami závislými na fields !
    5                   => array_merge($tabsInOnly[5], $tabsInOut[5]),
    6                   => array_merge($tabsInOnly[6], $tabsInOut[6])
];
$tabs_InOut_OutOnly = [      
    5                   => array_merge($tabsInOut[5], $tabsOutOnly[5]),
    6                   => array_merge($tabsInOut[6], $tabsOutOnly[6])
];
$tabsList_InOut_InOnly = [
    5                   => array_keys($tabs_InOut_InOnly[5]),
    6                   => array_keys($tabs_InOut_InOnly[6])
];
$tabsList_InOut_OutOnly = [
    5                   => array_keys($tabs_InOut_OutOnly[5]),
    6                   => array_keys($tabs_InOut_OutOnly[6])
];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// pole obsahující unikátní seznam výstupních tabulek všech verzí Daktely s počtem sloupců jednotlivých tabulek
$outTabsColsCount = [];
foreach ($tabs_InOut_OutOnly as $verTabs) {                     // iterace podle verzí Daktely (klíč = 5, 6, ...)
    foreach ($verTabs as $tab => $cols) {                       // iterace definic tabulek v rámci dané verze
        if (!array_key_exists($tab, $outTabsColsCount)) {
            $outTabsColsCount[$tab] = count($cols);
        }
    } 
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// seznam výstupních tabulek, u kterých požadujeme mít ID a hodnoty společné pro všechny instance
                // "název_tabulky" => 0/1 ~ vypnutí/zapnutí volitelného požadavku na indexaci záznamů v tabulce společnou pro všechny instance
$instCommonOuts = ["statuses" => 1, "groups" => 1, "fieldValues" => 1];
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// ostatní proměnné

// volitelná náhrada prázdných hodnot ID umělou hodnotou ID, která odpovídá umělému title
// motivace:  pro joinování tabulek v GD (tam se prázdná hodnota defaultně označuje jako "(empty value)")
$emptyToNA   = true;
$fakeId      = "n/a";
$fakeTitle   = "(empty value)";
$tabsFakeRow = ["users", "statuses"];

// počty číslic, na které jsou doplňovány ID's (kvůli řazení v GoodData je výhodné mít konst. délku ID's) a oddělovač prefixu od hodnoty
$idFormat = [
    "sep"       =>  "",                                     // znak oddělující ID instance od inkrementálního ID dané tabulky ("", "-" apod.)
    "instId"    =>  ceil(log10(max(2, count($instances)))), // počet číslic, na které je doplňováno ID instance (hodnota před oddělovačem) - určuje se dle počtu instancí
    "idTab"     =>  8,                                      // výchozí počet číslic, na které je doplňováno inkrementální ID dané tabulky (hodnota za oddělovačem);
                                                            // příznakem potvrzujícím, že hodnota dostačovala k indexaci záznamů u všech tabulek, je proměnná $idFormatIdEnoughDigits;
                                                            // nedoplňovat = "" / 0 / NULL / []  (~ hodnota, kterou lze vyhodnotit jako empty)    
    "idField"   =>  3                                       // výchozí počet číslic, na které je doplňováno inkrementální ID hodnot konkrétního form. pole
];

// delimitery názvu skupiny v queues.idgroup
$delim = [ "L" => "[[" , "R" => "]]" ];

// proměnná "action" typu ENUM u campaignRecords - převodní pole číselných kódů akcí na názvy akcí
$campRecordsActions = [
    "0" => "Not assigned",
    "1" => "Ready",
    "2" => "Called",
    "3" => "Call in progress",
    "4" => "Hangup",
    "5" => "Done",
    "6" => "Rescheduled"
];

// klíčová slova pro identifikaci typů formulářových polí a pro validaci + konverzi obsahu formulářových polí
$keywords = [
    "dateEq" => ["od", "do"],
    "mailEq" => ["mail", "email", "e-mail"],
    "date"   => ["datum"],
    "name"   => ["jméno", "jmeno", "příjmení", "prijmeni", "řidič", "ceo", "makléř", "předseda"],
    "addr"   => ["adresa", "address", "město", "mesto", "obec", "část obce", "ulice", "čtvrť", "ctvrt", "okres"],
    "psc"    => ["psč", "psc"],
    "addrVal"=> ["do","k","ke","mezi","na","nad","pod","před","při","pri","u","ve","za","čtvrť","ctvrt","sídliště","sidliste","sídl.","sidl.",
                 "ulice","ul.","třída","trida","tř.","tr.","nábřeží","nábř.","nabrezi","nabr.","alej","sady","park","provincie","svaz","území","uzemi",
                 "království","kralovstvi","republika","stát","stat","ostrovy", "okr.","okres","kraj", "kolonie","č.o.","c.o.","č.p.","c.p."],
                 // místopisné předložky a označení
    "romnVal"=> ["i", "ii", "iii", "iv", "vi", "vii", "viii", "ix", "x", "xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx"],
    "noConv" => ["v"]   // nelze rozhodnout mezi místopis. předložkou a řím. číslem → nekonvertovat case    
];
// ==============================================================================================================================================================================================
// funkce
                                                            // prefixování hodnoty atributu identifikátorem instance + nastavení požadované délky num. řetězců
function setIdLength ($instId = 0, $str, $useInstPref = true, $objType = "tab") { 
    global $idFormat;
    $len = $objType=="tab" ? $idFormat["idTab"] : $idFormat["idField"]; // jde o ID položky tabuky / ID form. pole (objType = tab / fielf)
    switch (!strlen($str)) {
        case true:  return "";                              // vstupní hodnota je řetězec nulové délky
        case false: $idFormated = !empty($len) ? sprintf('%0'.$len.'s', $str) : $str;
                    switch ($useInstPref) {                 // true = prefixovat hodnotu identifikátorem instance a oddělovacím znakem
                        case true:  return sprintf('%0'.$idFormat["instId"].'s', $instId) . $idFormat["sep"] . $idFormated;
                        case false: return $idFormated;    
                    }   
    }
}                                                       // prefixují se jen vyplněné hodnoty (strlen > 0)
function logInfo ($text, $dumpLevel="basicStatusInfo") {// volitelné diagnostické výstupy do logu
    global $diagOutOptions;
    $dumpKey = array_key_exists($dumpLevel, $diagOutOptions) ? $dumpLevel : "basicStatusInfo";
    echo $diagOutOptions[$dumpKey] ? $text."\n" : "";
}
function groupNameParse ($str) {                        // separace názvu skupiny jako podřetězce ohraničeného definovanými delimitery z daného řetězce
    global $delim;
    $match = [];                                        // "match array"
    preg_match("/".preg_quote($delim["L"])."(.*?)".preg_quote($delim["R"])."/s", $str, $match);
    return empty($match[1]) ?  "" : $match[1];          // $match[1] obsahuje podřetězec ohraničený delimitery ($match[0] dtto včetně delimiterů)
}
function phoneNumberCanonic ($str) {                    // veřejná tel. čísla omezená na číslice 0-9 (48-57D = 30-39H), bez úvodních nul (ltrim)
    $strConvert = ltrim(preg_replace("/[\\x00-\\x2F\\x3A-\\xFF]/", "", $str), "0");
    return (strlen($strConvert) == 9 ? "420" : "") . $strConvert;
}
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
function substrInStr ($str, $substr) {                                          // test výskytu podřetězce v řetězci
    return strlen(strstr($str, $substr)) > 0;                                   // vrací true / false
}
function mb_ucwords ($str) {                                                    // ucwords pro multibyte kódování
    return mb_convert_case($str, MB_CASE_TITLE, "UTF-8");
}
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
function remStrMultipl ($str, $delimiter = " ") {                               // převod multiplicitních podřetězců v řetězci na jeden výskyt podřetězce
    return implode($delimiter, array_unique(explode($delimiter, $str)));
}
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
function convertMail ($mail) {                                                  // validace e-mailové adresy a převod na malá písmena
    $mail = strtolower($mail);                                                  // převod e-mailové adresy na malá písmena
    $isValid = !(!filter_var($mail, FILTER_VALIDATE_EMAIL));                    // validace e-mailové adresy
    return $isValid ? $mail : "(nevalidní e-mail) ".$mail;                      // vrátí buď e-mail (lowercase), nebo e-mail s prefixem "(nevalidní e-mail) "
}
function convertPSC ($str) {                                                    // vrátí buď PSČ ve tvaru xxx xx (validní), nebo "nevalidní PSČ ve formuláři"
    $str = str_replace(" ", "", $str);                                          // odebrání mezer => pracovní tvar validního PSČ je xxxxx
    return (is_numeric($str) && strlen($str) == 5) ? substr($str, 0, 3)." ".substr($str, 3, 2) : "nevalidní PSČ ve formuláři";  // finální tvar PSČ je xxx xx
}
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
function boolValsUnify ($val) {         // dvojici booleovských hodnot ("",1) u v6 převede na dvojici hodnot (0,1) používanou u v5 (lze použít u booleovských atributů)
    global $inst;
    switch ($inst["ver"]) {
        case 5: return $val;                    // v5 - hodnoty 0, 1 → propíší se
        case 6: return $val=="1" ? $val : "0";  // v6 - hodnoty "",1 → protože jde o booleovskou proměnnou, nahradí se "" nulami
    }
}
function actionCodeToName ($actCode) {  // atribut "action" typu ENUM - převod číselného kódu akce na název akce
    global $campRecordsActions;
    return array_key_exists($actCode, $campRecordsActions) ? $campRecordsActions[$actCode] : $actCode;
}
function setFieldsShift () {            // posun v ID formCrmFields oproti ID formFields
    global $formFieldsIdShift, $formCrmFieldsIdShift, $idFormat;
    $formFieldsIdShift    = 0;
    $formCrmFieldsIdShift = pow(10, $idFormat["idTab"] - 1);   // přidá v indexu číslici 1 na první pozici zleva (číslice nejvyššího řádu)
}
function initGroups () {                // nastavení výchozích hodnot proměnných popisujících skupiny
    global $groups, $idGroup;
    $groups             = [];           // 1D-pole skupin - prvek pole má tvar groupName => idgroup
    $idGroup            = 0;            // umělý inkrementální index pro číslování skupin
}
function initStatuses () {              // nastavení výchozích hodnot proměnných popisujících stavy
    global $statuses, $idStatus, $idstatusFormated;
    $statuses = [];                     /* 3D-pole stavů - prvek pole má tvar  <statusId> => ["title" => <hodnota>, "statusIdOrig" => [pole hodnot]],
                                           kde statusId a title jsou unikátní, statusId jsou neformátované indexy (bez prefixu instance, který v commonStatus
                                           režimu nemá význam, a bez formátování na počet číslic požadovaný ve výstupních tabulkách)
                                           a v poli statusIdOrig jsou originální (prefixované) ID stejnojmenných stavů z různých instancí  */
    $idStatus             = 0;          // umělý inkrementální index pro číslování stavů (1, 2, ...)
    unset($idstatusFormated);           // formátovaný umělý index stavu ($idStatus doplněný na počet číslic požadovaný ve výstupních tabulkách)
}
function initFields () {                // nastavení výchozích hodnot proměnných popisujících formulářová pole
    global $fields;
    $fields = [];                       // 2D-pole formulářových polí - prvek pole má tvar <name> => ["idfield" => <hodnota>, "title" => <hodnota>]    
}
function iterStatuses ($val, $valType = "statusIdOrig") {   // prohledání 3D-pole stavů $statuses
    global $statuses;                   // $val = hledaná hodnota;  $valType = "title" / "statusIdOrig"
    foreach ($statuses as $statId => $statRow) {
        switch ($valType) {
            case "title":           // $statRow[$valType] je string
                                    if ($statRow[$valType] == $val) {   // zadaná hodnota v poli $statuses nalezena
                                        return $statId;                 // ... → vrátí id (umělé) položky pole $statuses, v níž se hodnota nachází
                                    }
                                    break;
            case "statusIdOrig":    // $statRow[$valType] je 1D-pole
                                    foreach ($statRow[$valType] as $statVal) {
                                        if ($statVal == $val) {     // zadaná hodnota v poli $statuses nalezena
                                            return $statId;         // ... → vrátí id (umělé) položky pole $statuses, v níž se hodnota nachází
                                        }
                                    }
        }        
    }
    return false;                   // zadaná hodnota v poli $statuses nenalezena
}
function callTimeRngCheck ($val) {
    global $incrementalOn, $incremHistDays; 
    if ($incrementalOn &&           // je-li u tabulky 'calls' požadován jen inkrementální výstup (hovory novější než...) ...
        substr($val, 0, 10) < date("Y-m-d", strtotime(-$incremHistDays." days"))) { // ... pak je-li daný hovor starší než... ($val je datumočas) ...   
            return false;
    }        
    return true;
}
function emptyToNA ($id) {          // prázdné hodnoty nahradí hodnotou $fakeId - kvůli GoodData, aby zde byla nabídka $fakeTitle [volitelné]
    global $emptyToNA, $fakeId;
    return ($emptyToNA && empty($id)) ? $fakeId : $id;
}
function checkIdLengthOverflow ($val) {     // kontrola, zda došlo (true) nebo nedošlo (false) k přetečení délky ID určené proměnnou $idFormat["idTab"] ...
    global $idFormat, $tab;                 // ... nebo umělým ID (groups, statuses, fieldValues)
        if ($val >= pow(10, $idFormat["idTab"])) {
            logInfo("PŘETEČENÍ DÉLKY INDEXŮ ZÁZNAMŮ V TABULCE ".$tab);          // volitelný diagnostický výstup do logu                
            $idFormat["idTab"]++;
            logInfo("DÉLKA INDEXŮ NAVÝŠENA NA ".$idFormat['idTab']." ČÍSLIC");
            return true;                    // došlo k přetečení → je třeba začít plnit OUT tabulky znovu, s delšími ID
        }
    return false;                           // nedošlo k přetečení (OK)
}
function jsonParse ($formArr) {     // formArr je 2D-pole    
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
function colParentTab ($instId, $tab, $colName) {                               // nalezení názvu nadřazené tabulky pro daný sloupec (je-li sloupec FK)
    global $fkList, $diagOutOptions;
    if (array_key_exists($instId, $fkList)) {
        if (array_key_exists($tab, $fkList[$instId])) {
            if (array_key_exists($colName, $fkList[$instId][$tab])) {
                $parenTbl = $fkList[$instId][$tab][$colName];
                return $parenTbl;                                               // daný sloupec je FK → vrátí název nadřazené tabulky
            }
        }
    }                                                                           // daný sloupec není FK → vrátí NULL
}
function integrityValid ($instId, $tab, $colName, $unprefixVal) {               // integritní validace
    global $pkVals, $diagOutOptions;
    $colParentTab = colParentTab($instId, $tab, $colName);                      // název nadřazené tabulky u sloupce, který je FK 
    if (is_null($colParentTab)) {return true;}                                  // daný sloupec není FK → vrátí true                                                                              
    if (array_key_exists($instId, $pkVals)) {                                   // test existance odpovídajícího záznamu v nadřazené tabulce
        if (array_key_exists($colParentTab, $pkVals[$instId])) {
            if (array_key_exists($colName, $pkVals[$instId][$colParentTab])) {
                if (array_key_exists($unprefixVal, $pkVals[$instId][$colParentTab][$colName])) {
                    return true;                                                // hodnota $unprefixVal byla nalezena v hodnotách PK nadřazené tabulky
                } else {
                    logInfo("INSTANCE ".$instId.": HODNOTA ".$tab.".".$colName." = ".$unprefixVal." NEMÁ NADŘAZENÝ ZÁZNAM V TABULCE ".$colParentTab." -> NEBUDE PROPSÁNA NA VÝSTUP", "detailIntegrInfo");
                    return false;                                               // hodnota $unprefixVal nebyla nalezena v hodnotách PK nadřazené tabulky
                }
            }
        }
    }                                                                           // v poli $pkVals nenalezen některý z potřebných klíčů → o integritní správnosti nelze rozhodnout (vrátí NULL)
}
function tabItemsIncrem ($colName, $integrValidResult) {                        // inkrement počitadel vstupních záznamů všech/vyhovujících/nevyhovujících integritní validaci
    global $tabItems;                                                           // $integrValidResult = "integrOk" / "integrErr"
    // test existence potřebných počitadel v poli $tabItems, založení chybějících počitadel s nulovou hodnotou
    if(!array_key_exists($colName, $tabItems))              {$tabItems[$colName] = [];}
    if(!array_key_exists("total" , $tabItems))              {$tabItems["total"]  = [];}
    if(!array_key_exists("integrOk" , $tabItems[$colName])) {$tabItems[$colName]["integrOk"] = 0;}
    if(!array_key_exists("integrErr", $tabItems[$colName])) {$tabItems[$colName]["integrErr"]= 0;}
    if(!array_key_exists("integrOk" , $tabItems["total"] )) {$tabItems["total"] ["integrOk"] = 0;}
    if(!array_key_exists("integrErr", $tabItems["total"] )) {$tabItems["total"] ["integrErr"]= 0;}
    // inkrement požadovaných počitadel
    $tabItems[$colName][$integrValidResult]++;
    $tabItems["total"] [$integrValidResult]++;    
}
logInfo("PROMĚNNÉ A FUNKCE ZAVEDENY");                                          // volitelný diagnostický výstup do logu
// ==============================================================================================================================================================================================
// načtení vstupních souborů
foreach ($instances as $instId => $inst) {
    foreach ($tabsList_InOut_InOnly[$inst["ver"]] as $file) {
        ${"in_".$file."_".$instId} = new Keboola\Csv\CsvFile($dataDir."in".$ds."tables".$ds."in_".$file."_".$instId.".csv");
    }
}
logInfo("VSTUPNÍ SOUBORY NAČTENY");     // volitelný diagnostický výstup do logu
// ==============================================================================================================================================================================================
logInfo("ZAHÁJENO PROHLEDÁNÍ VSTUPNÍCH SOUBORŮ (KONTROLA POČTU ZÁZNAMŮ + PODKLADY PRO INTEGRITNÍ VALIDACI)");   // volitelný diagnostický výstup do logu
/*$pkList =*/ $pkVals = $fkList = [];
/* struktura polí:  //$pkList = [$instId => [$tab => <název_PK>]]                             ... pole názvů PK pro vst. tabulky
                    $pkVals = [$instId => [$tab => [<pole existujících hodnot PK>]]]        ... pole existujících hodnot PK pro vst. tabulky
                    $fkList = [$instId => [$tab => [$colName => <název_nadřazené_tabulky>]]]... pole názvů nadřazených tabulek pro každý sloupec, který je FK
*/
foreach ($instances as $instId => $inst) {                                      // iterace instancí
    logInfo("ZAHÁJENO PROHLEDÁVÁNÍ INSTANCE ".$instId);                         // volitelný diagnostický výstup do logu        
    
    foreach ($tabs_InOut_InOnly[$inst["ver"]] as $tab => $cols) {               // iterace tabulek; $tab - název tabulky, $cols - pole s parametry sloupců
        logInfo("ZAHÁJENO PROHLEDÁVÁNÍ TABULKY ".$tab." Z INSTANCE ".$instId);  // volitelný diagnostický výstup do logu   
        
        $colId = 0;                                                             // počitadlo sloupců (číslováno od 0)
        $pkColId = NULL;                                                        // ID sloupce, který je pro danou tabulku PK (číslováno od 0; NULL - PK nenelezen)
        foreach ($cols as $colName => $colAttrs) {                              // iterace sloupců
            if (is_null($pkColId) && array_key_exists("pk", $colAttrs)) {       // dosud prohledané sloupce nebyly PK / nalezen sloupec, který je PK
                //$pkList[$instId][$tab] = $colName;                            // uložení názvu PK do pole $pkList
                $pkColId = $colId;                                              //
                logInfo($tab." Z INSTANCE ".$instId." - PK NALEZEN :-)");
            }
            if (array_key_exists("fk", $colAttrs)) {                            // nalezen sloupec, který je PK
                $fkList[$instId][$tab][$colName] = $colAttrs["fk"];             // uložení názvu nadřezené tabulky do pole $fkList                                         //
                logInfo($tab." Z INSTANCE ".$instId." - NALEZEN FK DO TABULKY ".$colAttrs["fk"]);
            } 
            $colId ++;                                                          // přechod na další sloupec            
        }
        
        if (is_null($pkColId)) {
            logInfo($tab." Z INSTANCE ".$instId." - NEBYL NALEZEN PK ;-o");
            continue;                                                           // nepokračuje se iterací řádků a načtením hodnot PK do pole, ...
        }                                                                       // ... přejde se rovnou na další tabulku
        // shromáždění hodnot PK z dané tabulky
        foreach (${"in_".$tab."_".$instId} as $rowNum => $row) {                // iterace řádků vst. tabulek; $rowNum - ID řádku, $row - pole hodnot
            if ($rowNum == 0) {continue;}            
            $pkVals[$instId][$tab][] = $row[$pkColId];                          // uložení hodnoty PK do pole $pkVals
        }
        $pkVals[$instId][$tab] = array_values(array_unique($pkVals[$instId][$tab]));// eliminace případných multiplicit hodnot PK (ale neměly by se vyskytovat)
        $pkValsTabCnt = count($pkVals[$instId][$tab]);                          // počet unikátních hodnot PK pro danou tabulku  
        checkIdLengthOverflow($pkValsTabCnt);                                   // při překročení kapacity navýší délku inkrementálních indexů o 1 číslici
        logInfo("V TABULCE ".$tab." Z INSTANCE ".$instId." NALEZENO ".$pkValsTabCnt." ZÁZNAMŮ");
    }
}
logInfo("DOKONČENO PROHLEDÁNÍ VSTUPNÍCH SOUBORŮ (KONTROLA POČTU ZÁZNAMŮ + PODKLADY PRO INTEGRITNÍ VALIDACI)");
logInfo("PŘEDPOKLÁDANÁ DÉLKA INDEXŮ VE VÝSTUPNÍCH TABULKÁCH JE ".$idFormat['instId']+ $idFormat['idTab']." ČÍSLIC");  // volitelný diagnostický výstup do logu
// ==============================================================================================================================================================================================
logInfo("ZAHÁJENO ZPRACOVÁNÍ DAT");     // volitelný diagnostický výstup do logu
$idFormatIdEnoughDigits = false;        // příznak potvrzující, že počet číslic určený proměnnou $idFormat["idTab"] dostačoval k indexaci záznamů u všech tabulek (vč. out-only položek)
while (!$idFormatIdEnoughDigits) {      // dokud není potvrzeno, že počet číslic určený proměnnou $idFormat["idTab"] dostačoval k indexaci záznamů u všech tabulek (vč. out-only položek)
   
    foreach ($tabs_InOut_OutOnly[6] as $tab => $cols) {        
        // vytvoření výstupních souborů    
        ${"out_".$tab} = new \Keboola\Csv\CsvFile($dataDir."out".$ds."tables".$ds."out_".$tab.".csv");
        // zápis hlaviček do výstupních souborů
        $colsOut = array_key_exists($tab, $colsInOnly) ? array_diff(array_keys($cols), $colsInOnly[$tab]) : array_keys($cols);
        $colPrf  = strtolower($tab)."_";                    // prefix názvů sloupců ve výstupní tabulce (např. "loginSessions" → "loginsessions_")
        $colsOut = preg_filter("/^/", $colPrf, $colsOut);   // prefixace názvů sloupců ve výstupních tabulkách názvy tabulek kvůli rozlišení v GD (např. "title" → "groups_title")
        ${"out_".$tab} -> writeRow($colsOut); 
    }
    logInfo("VÝSTUPNÍ SOUBORY VYTVOŘENY, ZÁHLAVÍ VLOŽENA"); // volitelný diagnostický výstup do logu

    // vytvoření záznamů s umělým ID v tabulkách definovaných proměnnou $tabsFakeRow (kvůli JOINu tabulek v GoodData) [volitelné]
    if ($emptyToNA) {
        foreach ($tabsFakeRow as $ftab) {
            $frow = array_merge([$fakeId, $fakeTitle], array_fill(2, $outTabsColsCount[$ftab] - 2, ""));
            ${"out_".$ftab} -> writeRow($frow);
            logInfo("VLOŽEN UMĚLÝ ZÁZNAM S ID ".$fakeId." A NÁZVEM ".$fakeTitle." DO VÝSTUPNÍ TABULKY ".$ftab); // volitelný diag. výstup do logu
        }               // umělý řádek do aktuálně iterované tabulky ... ["n/a", "(empty value"), "", ... , ""]          
    }
    // ==========================================================================================================================================================================================
    // zápis záznamů do výstupních souborů

    // [A] tabulky sestavené ze záznamů více instancí (záznamy ze všech instancí se zapíší do stejných výstupních souborů

    setFieldsShift();                                       // výpočet konstant posunu indexování formulářových polí
    initStatuses();                                         // nastavení výchozích hodnot proměnných popisujících stavy
    initGroups();                                           // nastavení výchozích hodnot proměnných popisujících skupiny

    foreach ($instCommonOuts as $tab => $common) {
        switch ($common) {
            case 0: ${"common".ucfirst($tab)}=false; break; // záznamy v tabulce budou indexovány pro každou instanci zvlášť
            case 1: ${"common".ucfirst($tab)}=true;         // záznamy v tabulce budou indexovány pro všechny instance společně
        }
    }

    foreach ($instances as $instId => $inst) {              // procházení tabulek jednotlivých instancí Daktela
        initFields();                                       // nastavení výchozích hodnot proměnných popisujících formulářová pole         
        if (!$commonStatuses)    {initStatuses();   }       // ID a názvy v tabulce 'statuses' požadujeme uvádět pro každou instanci zvlášť    
        if (!$commonGroups)      {initGroups();     }       // ID a názvy v out-only tabulce 'groups' požadujeme uvádět pro každou instanci zvlášť
        if (!$commonFieldValues) {initFieldValues();}       // ID a titles v tabulce 'fieldValues' požadujeme uvádět pro každou instanci zvlášť
        logInfo("ZAHÁJENO ZPRACOVÁNÍ INSTANCE ".$instId);   // volitelný diagnostický výstup do logu

        foreach ($tabs_InOut_InOnly[$inst["ver"]] as $tab => $cols) {               // iterace tabulek dané instance
            logInfo("ZAHÁJENO ZPRACOVÁNÍ TABULKY ".$tab." Z INSTANCE ".$instId);    // volitelný diagnostický výstup do logu
            $tabItems = []; // pole počitadel vstupních záznamů všech/vyhovujících/nevyhovujících integritní validaci (sčítá se pro danou instanci a danou tabulku)
                            // struktura pole:  $integrValidCounts = [$colName1 => ["integrOk" => <počet>, "integrErr" => <počet>],
                            //                                        $colNameN => ["integrOk" => <počet>, "integrErr" => <počet>],
                            //                                        "total"   => ["integrOk" => <počet>, "integrErr" => <počet>] ]
            foreach (${"in_".$tab."_".$instId} as $rowNum => $row) {                // načítání řádků vstupních tabulek [= iterace řádků]
                if ($rowNum == 0) {continue;}                                       // vynechání hlavičky tabulky
                $colVals = $callsVals = $fieldRow = [];                             // řádek obecné výstupní tabulky | řádek výstupní tabulky 'calls' | záznam do pole formulářových polí     
                unset($idFieldSrcRec, $idstat, $idqueue, $iduser, $type);           // reset indexu zdrojového záznamu do out-only tabulky hodnot formulářových polí + ID stavů, front, uživatelů a typu aktivity
                $colId  = 0;                                                        // index sloupce (v každém řádku číslovány sloupce 0,1,2,...)
                
                foreach ($cols as $colName => $colAttrs) {                          // konstrukce řádku výstupní tabulky (vložení hodnot řádku) [= iterace sloupců]                    
                    // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    if (!integrityValid($instId, $tab, $colName, $row[$colId])) {   // integritní validace pro aktuální instanci, tabulku a sloupec (= test existence odpovídajícího záznamu v nadřazené tabulce)
                        tabItemsIncrem ($colName, "integrErr");
                        continue 2;                                                 // řádek nesplňuje podmínku integrity → nebude propsán do výstupní tabulky
                    }
                    tabItemsIncrem ($colName, "integrOk");
                    // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    switch ($colAttrs["instPrf"]) {                                 // prefixace hodnoty číslem instance (je-li požadována)
                        case 0: $hodnota = $row[$colId]; break;                     // hodnota bez prefixu instance
                        case 1: $hodnota = setIdLength($instId, $row[$colId]);      // hodnota s prefixem instance
                    }
                    // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    switch ([$tab, $colName]) {
                        // TABULKY V5+6
                        case ["pauses", "paid"]:    $colVals[] = boolValsUnify($hodnota);                       // dvojici bool. hodnot ("",1) u v6 převede na dvojici hodnot (0,1) používanou u v5                                 
                                                    break;
                        case ["queues", "idgroup"]: $groupName = groupNameParse($hodnota);                      // název skupiny parsovaný z queues.idgroup pomocí delimiterů
                                                    if (!strlen($groupName)) {                                  // název skupiny ve vstupní tabulce 'queues' nevyplněn ...
                                                        $colVals[] = "";  break;                                // ... → stav se do výstupní tabulky 'queues' nezapíše
                                                    }  
                                                    if (!array_key_exists($groupName, $groups)) {               // skupina daného názvu dosud není uvedena v poli $groups 
                                                        $idGroup++;                                             // inkrement umělého ID skupiny   
                                                        if (checkIdLengthOverflow($idGroup)) {                  // došlo k přetečení délky ID určené proměnnou $idGroup
                                                                continue 6;                                     // zpět na začátek cyklu 'while' (začít plnit OUT tabulky znovu, s delšími ID)
                                                            }
                                                        $idGroupFormated = setIdLength($instId,$idGroup,!$commonGroups);// $commonGroups → neprefixovat $idGroup identifikátorem instance
                                                        $groups[$groupName] = $idGroupFormated;                 // zápis skupiny do pole $groups
                                                        $out_groups -> writeRow([$idGroupFormated,$groupName]); // zápis řádku do out-only tabulky 'groups' (řádek má tvar idgroup | groupName)                                                                                                                                                              
                                                    } else {
                                                        $idGroupFormated = $groups[$groupName];                 // získání idgroup dle názvu skupiny z pole $groups
                                                    }                                                
                                                    $colVals[] = $idGroupFormated;                              // vložení formátovaného ID skupiny jako prvního prvku do konstruovaného řádku 
                                                    break;
                        case ["calls", "call_time"]:if (!callTimeRngCheck($hodnota)) {                          // 'call_time' není z požadovaného rozsahu -> ...
                                                        continue 3;                                             // ... řádek z tabulky 'calls' přeskočíme
                                                    } else {                                                    // 'call_time' je z požadovaného rozsahu -> ...
                                                        $colVals[] = $hodnota; break;                           // ... 'call_time' použijeme a normálně pokračujeme v konstrukci řádku...
                                                    }
                        case ["calls", "answered"]: $colVals[] = boolValsUnify($hodnota);                       // dvojici bool. hodnot ("",1) u v6 převede na dvojici hodnot (0,1) používanou u v5                                 
                                                    break;
                        case ["calls", "iduser"]:   $colVals[] = $emptyToNA && empty($hodnota) ? "n/a":$hodnota;// prázdné hodnoty nahradí "n/a" - kvůli GoodData, aby zde byla nabídka "(empty value)" [volitelné]                       
                                                    break;
                        case ["calls", "clid"]:     $colVals[] = phoneNumberCanonic($hodnota);                  // veřejné tel. číslo v kanonickém tvaru (bez '+')
                                                    break;
                        case ["calls", "contact"]:  $colVals[] = "";                                            // zatím málo využívané pole, obecně objekt (JSON), pro použití by byl nutný json_decode
                                                    break;
                        case["statuses","idstatus"]:if ($commonStatuses) {                                      // ID a názvy v tabulce 'statuses' požadujeme společné pro všechny instance  
                                                        $statIdOrig = $hodnota;                                 // uložení originálního (prefixovaného) ID stavu do proměnné $statIdOrig
                                                    } else {                                                    // ID a názvy v tabulce 'statuses' požadujeme uvádět pro každou instanci zvlášť
                                                        $colVals[]  = $hodnota;                                 // vložení formátovaného ID stavu jako prvního prvku do konstruovaného řádku
                                                    }              
                                                    break;
                        case ["statuses", "title"]: if ($commonStatuses) {                                      // ID a názvy v tabulce 'statuses' požadujeme společné pro všechny instance
                                                        $iterRes = iterStatuses($hodnota, "title");             // výsledek hledání title v poli $statuses (umělé ID stavu nebo false)
                                                        if (!$iterRes) {                                        // stav s daným title dosud v poli $statuses neexistuje
                                                            $idStatus++;                                        // inkrement umělého ID stavů
                                                            if (checkIdLengthOverflow($idStatus)) {             // došlo k přetečení délky ID určené proměnnou $idStatus
                                                                continue 6;                                     // zpět na začátek cyklu 'while' (začít plnit OUT tabulky znovu, s delšími ID)
                                                            }
                                                            $statuses[$idStatus]["title"]          = $hodnota;  // zápis hodnot stavu do pole $statuses
                                                            $statuses[$idStatus]["statusIdOrig"][] = $statIdOrig;
                                                            $colVals[] = setIdLength(0, $idStatus, false);      // vložení formátovaného ID stavu jako prvního prvku do konstruovaného řádku                                        

                                                        } else {                                                // stav s daným title už v poli $statuses existuje
                                                            $statuses[$iterRes]["statusIdOrig"][] = $statIdOrig;// připsání orig. ID stavu jako dalšího prvku do vnořeného 1D-pole ve 3D-poli $statuses
                                                            break;                                              // aktuálně zkoumaný stav v poli $statuses už existuje
                                                        }
                                                        unset($statIdOrig);                                     // unset proměnné s uloženou hodnotou originálního (prefixovaného) ID stavu (úklid)
                                                    }                                             
                                                    $colVals[] = $hodnota;                                      // vložení title stavu jako druhého prvku do konstruovaného řádku                                           
                                                    break;
                        case ["recordSnapshots", "idstatus"]:
                                                    $colVals[] = $commonStatuses ? setIdLength(0, iterStatuses($hodnota), false) : $hodnota;
                                                    break;
                        case ["fields", "idfield"]: $hodnota_shift = (int)$hodnota + $formFieldsIdShift;
                                                    $colVals[] = $fieldRow["idfield"] = $hodnota_shift;         // hodnota záznamu do pole formulářových polí
                                                    break;
                        case ["fields", "title"]:   $colVals[] = $fieldRow["title"]= $hodnota;                  // hodnota záznamu do pole formulářových polí
                                                    break;
                        case ["fields", "name"]:    $fieldRow["name"] = $hodnota;               // název klíče záznamu do pole formulářových polí
                                                    break;                                      // sloupec "name" se nepropisuje do výstupní tabulky "fields"                
                        case ["records","idrecord"]:$idFieldSrcRec = $colVals[] = $hodnota;     // uložení hodnoty 'idrecord' pro následné použití ve 'fieldValues'
                                                    break;
                        case ["records","idstatus"]:$idstat = $commonStatuses ? setIdLength(0, iterStatuses($hodnota), false) : $hodnota;
                                                    $colVals[] = emptyToNA($idstat);            // prázdné hodnoty nahradí $fakeId - kvůli GoodData, aby zde byla nabídka $fakeTitle [volitelné]  
                                                    break;
                        case ["records", "number"]: $colVals[] = phoneNumberCanonic($hodnota);  // veřejné tel. číslo v kanonickém tvaru (bez '+')
                                                    break;
                        case ["records", "action"]: $colVals[] = actionCodeToName($hodnota);    // číselný kód akce převedený na název akce
                                                    break;                                               
                        case ["records", "form"]:   $formArr = json_decode($hodnota, true, JSON_UNESCAPED_UNICODE);
                                                    if (is_null($formArr)) {break;} // hodnota dekódovaného JSONu je null → nelze ji prohledávat jako pole
                                                    jsonParse($formArr);                                         
                                                    break;                          // sloupec "form" se nepropisuje do výstupní tabulky "records"  
                        case [$tab,"idinstance"]:   $colVals[] = $instId;  break;   // hodnota = $instId    
                        // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------                                          
                        // TABULKY V6 ONLY
                        case ["contacts","idcontact"]:$idFieldSrcRec = $colVals[]= $hodnota;// uložení hodnoty 'idcontact' pro následné použití v 'contFieldVals'
                                                    break;
                        case ["contacts", "form"]:  $formArr = json_decode($hodnota, true, JSON_UNESCAPED_UNICODE);
                                                    // ------------------------------------------------------------------------------------------------------------------------------------------
                                                    // parsování "number" (veřejného tel. číslo) pro potřeby CRM records reportu
                                                    $telNum = "";
                                                    if (array_key_exists("number", $formArr)) {
                                                        if (array_key_exists(0, $formArr["number"])) {
                                                            $telNum = phoneNumberCanonic($formArr["number"][0]);    // uložení tel. čísla do proměnné $telNum
                                                        }                           // $contactsForm["number"] ... obecně 1D-pole, kde může být více tel. čísel → beru jen první
                                                    }
                                                    // ------------------------------------------------------------------------------------------------------------------------------------------
                                                    // parsování celého JSONu s hodnotami formulářových polí
                                                    if (is_null($formArr)) {break;} // hodnota dekódovaného JSONu je null → nelze ji prohledávat jako pole
                                                    jsonParse($formArr);
                                                    break;                          // sloupec "form" se nepropisuje do výstupní tabulky "contacts"  
                        case ["contacts","number"]: $colVals[] = $telNum;           // hodnota vytvořená v case ["contacts", "form"]
                                                    break;
                        case ["tickets","idticket"]:$idFieldSrcRec = $colVals[] = $hodnota; // uložení hodnoty 'idticket' pro následné použití v 'tickFieldVals'
                                                    break;
                        case ["tickets", "email"]:  $colVals[] = convertMail($hodnota);
                                                    break;
                        case ["tickets","idstatus"]:$colVals[] = $commonStatuses ? setIdLength(0, iterStatuses($hodnota), false) : $hodnota;
                                                    break;
                        case ["tickets", "form"]:   $formArr = json_decode($hodnota, true, JSON_UNESCAPED_UNICODE);
                                                    if (is_null($formArr)) {break;} // hodnota dekódovaného JSONu je null → nelze ji prohledávat jako pole
                                                    jsonParse($formArr);
                                                    break;                          // sloupec "form" se nepropisuje do výstupní tabulky "tickets"
                        case ["crmRecords", "idcrmrecord"]:$idFieldSrcRec = $colVals[]= $hodnota;   // uložení hodnoty 'idcrmrecord' pro následné použití v 'crmFieldVals'
                                                    break;
                        case ["crmRecords", "idstatus"]:
                                                    $colVals[] = $commonStatuses ? setIdLength(0, iterStatuses($hodnota), false) : $hodnota;
                                                    break;
                        case ["crmRecords", "form"]:$formArr = json_decode($hodnota, true, JSON_UNESCAPED_UNICODE);
                                                    if (is_null($formArr)) {break;} // hodnota dekódovaného JSONu je null → nelze ji prohledávat jako pole
                                                    jsonParse($formArr);
                                                    break;                          // sloupec "form" se nepropisuje do výstupní tabulky "crmRecords"
                        case ["crmRecordSnapshots", "idstatus"]:
                                                    $colVals[] = $commonStatuses ? setIdLength(0, iterStatuses($hodnota), false) : $hodnota;
                                                    break;
                        case ["activities", "idqueue"]:
                                                    $colVals[]= $idqueue= $hodnota; // $idqueue ... pro použití v case ["activities", "item"]
                                                    break;
                        case ["activities", "iduser"]:
                                                    $colVals[]= $iduser = $hodnota; // $iduser  ... pro použití v case ["activities", "item"]
                                                    break;
                        case ["activities", "idstatus"]:
                                                    $colVals[] = $commonStatuses ? setIdLength(0, iterStatuses($hodnota), false) : $hodnota;
                                                    break;
                        case ["activities", "type"]:$colVals[]= $type = $hodnota;   // $type ... pro použití v case ["activities", "item"]
                                                    break;                        
                        case ["activities", "item"]:$colVals[] = $hodnota;          // obecně objekt (JSON), propisováno do OUT bucketu i bez parsování (potřebuji 'duration' v performance reportu)
                                                    if ($type != "CALL") {break;}   // pro aktivity typu != CALL nepokračovat sestavením hodnot do tabulky 'calls'
                                                    $item = json_decode($hodnota, true, JSON_UNESCAPED_UNICODE);
                                                    if (is_null($item)) {break;}    // hodnota dekódovaného JSONu je null → nelze ji prohledávat jako pole

                                                    // příprava hodnot do řádku výstupní tabulky 'calls':
                                                    if (!callTimeRngCheck($item["call_time"])) {continue 3;} // 'call_time' není z požadovaného rozsahu -> řádek z tabulky 'activities' přeskočíme
                                                    $iduser = emptyToNA($iduser);   // prázdné hodnoty nahradí $fakeId - kvůli GoodData, aby zde byla nabídka $fakeTitle [volitelné]

                                                    $callsVals = [  $item["id_call"],                       // konstrukce řádku výstupní tabulky 'calls'
                                                                    $item["call_time"],
                                                                    $item["direction"],
                                                                    boolValsUnify($item["answered"]),
                                                                    $idqueue,
                                                                    $iduser,
                                                                    phoneNumberCanonic($item["clid"]),
                                                                    $item["contact"]["_sys"]["id"],
                                                                    $item["did"],
                                                                    $item["wait_time"],
                                                                    $item["ringing_time"],
                                                                    $item["hold_time"],
                                                                    $item["duration"],
                                                                    $item["orig_pos"],
                                                                    $item["position"],
                                                                    $item["disposition_cause"],
                                                                    $item["disconnection_cause"],
                                                                    $item["pressed_key"],
                                                                    $item["missed_call"],
                                                                    $item["missed_call_time"],
                                                                    $item["score"],
                                                                    $item["note"],
                                                                    $item["attemps"],
                                                                    $item["qa_user_id"],
                                                                    $instId
                                                    ];
                                                    if (!empty($callsVals)) {                           // je sestaveno pole pro zápis do řádku výstupní tabulky 'calls'
                                                        $out_calls -> writeRow($callsVals);             // zápis sestaveného řádku do výstupní tabulky 'calls'
                                                    }
                                                    break; 
                        case ["crmFields", "idcrmfield"]:
                                                    $hodnota_shift = (int)$hodnota + $formCrmFieldsIdShift;
                                                    $colVals[] = $fieldRow["idfield"] = $hodnota_shift; // hodnota záznamu do pole formulářových polí
                                                    break;
                        case ["crmFields", "title"]:$colVals[] = $fieldRow["title"] = $hodnota;         // hodnota záznamu do pole formulářových polí
                                                    break;
                        case ["crmFields", "name"]: $fieldRow["name"] = $hodnota;                       // název klíče záznamu do pole formulářových polí
                                                    break;                                              // sloupec "name" se nepropisuje do výstupní tabulky "fields"                      
                        // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                  
                        default:                    $colVals[] = $hodnota;          // propsání hodnoty ze vstupní do výstupní tabulky bez úprav (standardní mód)
                    }
                    $colId++;                                                       // přechod na další sloupec (buňku) v rámci řádku                
                }   // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------              
                // operace po zpracování dat v celém řádku

                // přidání řádku do pole formulářových polí $fields (struktura pole je <idfield> => ["name" => <hodnota>, "title" => <hodnota>] )
                if ( !(!strlen($fieldRow["name"]) || !strlen($fieldRow["idfield"]) || !strlen($fieldRow["title"])) ) {  // je-li známý název, title i hodnota záznamu do pole form. polí...          
                        /*if ($instId == "3" && ($tab == "crmFields" || $tab == "fields")) {
                        echo "do pole 'fields' přidán záznam (idfield ".$fieldRow["idfield"].", name ".$fieldRow["name"].", title ".$fieldRow["title"].")\n";
                        } */
                    $fields[$fieldRow["idfield"]]["name"]  = $fieldRow["name"];     // ... provede se přidání prvku <idfield>["name"] => <hodnota> ...
                    $fields[$fieldRow["idfield"]]["title"] = $fieldRow["title"];    // ... a prvku <idfield>["title"] => <hodnota>
                }    
                $tabOut = ($tab != "crmFields") ? $tab : "fields";                  // záznamy z in-only tabulky 'crmFields' zapisujeme do in-out tabulky 'fields' 

                if (!empty($colVals)) {                                             // je sestaveno pole pro zápis do řádku výstupní tabulky
                    ${"out_".$tabOut} -> writeRow($colVals);                        // zápis sestaveného řádku do výstupní tabulky
                }
            }   // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            // operace po zpracování dat v celé tabulce
            logInfo("DOKONČENO ZPRACOVÁNÍ TABULKY ".$tab." Z INSTANCE ".$instId);   // volitelný diagnostický výstup do logu
            if (empty($tabItems)) {continue;}                                       // v tabulce nikde nedošlo ke kontrole integritní validace → přechod k další tabulce
            logInfo("TABULKA ".$tab." Z INSTANCE ".$instId." - SOUHRN INTEGRITNÍ ÚSPĚŠNOSTI:");
            foreach ($tabItems as $colName => $colCounts) {
                switch ($colName) {
                    case "total": logInfo("CELKEM: ".$colCounts["integrOK"]." ZÁZNAMŮ INTEGRITNĚ OK, ".$colCounts["integrErr"]." ZÁZNAMŮ S CHYBĚJÍCÍM ZÁZNAMEM V NADŘAZENÉ TABULCE"); break;                  
                    default:      logInfo("SLOUPEC ".$colName.": ".$colCounts["integrOK"]." ZÁZNAMŮ INTEGRITNĚ OK, ".$colCounts["integrErr"]." ZÁZNAMŮ S CHYBĚJÍCÍM ZÁZNAMEM V NADŘAZENÉ TABULCE");  
                }
            }            
        }
        logInfo("DOKONČENO ZPRACOVÁNÍ INSTANCE ".$instId);                      // volitelný diagnostický výstup do logu
        // operace po zpracování dat ve všech tabulkách jedné instance
                        //echo "pole 'fields' instance ".$instId.":\n"; print_r($fields); echo "\n";        
    }
    // operace po zpracování dat ve všech tabulkách všech instancí

    // diagnostická tabulka - výstup pole $statuses
    $out_arrStat = new \Keboola\Csv\CsvFile($dataDir."out".$ds."tables".$ds."out_arrStat.csv");
    $out_arrStat -> writeRow(["id_status_internal", "title", "id_statuses_orig"]);
    foreach ($statuses as $statId => $statVals) {
        $colStatusesVals = [$statId, $statVals["title"], json_encode($statVals["statusIdOrig"])];
        $out_arrStat -> writeRow($colStatusesVals);
    }
    
    $idFormatIdEnoughDigits = true;         // potvrzení, že počet číslic určený proměnnou $idFormat["idTab"] dostačoval k indexaci záznamů u všech tabulek (vč. out-only položek)
}
// ==============================================================================================================================================================================================
// [B] tabulky společné pro všechny instance (nesestavené ze záznamů více instancí)
// instances
foreach ($instances as $instId => $inst) {
    $out_instances -> writeRow([$instId, $inst["url"]]);
}
logInfo("TRANSFORMACE DOKONČENA");          // volitelný diagnostický výstup do logu
?>