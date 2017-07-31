<?php
// proměnné a konstanty

// datumový rozsah zpracování
$processedDates     =   [   "start" =>  date("Y-m-d", strtotime(-$histDays['start']." days")),  // počáteční datum zpracováváného rozsahu
                            "end"   =>  date("Y-m-d", strtotime(-$histDays['end']  ." days"))       // koncové datum zpracovávaného rozsahu
                        ];

// počet dní, o které je rozšířen rozsah $processedDates pro načtení hodnot PK do pole $pkVals (použije se při integritní validaci)
$pkValsReserveDays  =   [   "start" =>  60,
                            "end"   => 5
                        ];
$pkValsHistDays     =   [   "start" =>  $histDays["start"] + $pkValsReserveDays["start"], 
                            "end"   =>  $histDays["end"]   + $pkValsReserveDays["end"]
                        ];
$pkValsProcessedDates = [   "start" =>  date("Y-m-d", strtotime(-$pkValsHistDays['start']." days")),
                            "end"   =>  date("Y-m-d", strtotime(-$pkValsHistDays['end']  ." days"))
                        ];

// seznam instancí Daktela
$instancesList = [  "1" =>  ["url" => "https://ilinky.daktela.com",       "ver" => 5],
                    "2" =>  ["url" => "https://dircom.daktela.com",       "ver" => 5],
                    "3" =>  ["url" => "https://conectart.daktela.com",    "ver" => 6],
                    "4" =>  ["url" => "https://conectart-in.daktela.com", "ver" => 6]
];
// z pole $instancesList se do pole $instances vyberou jen definice instancí, které mají v konfiguračním JSONu zapnuté zpracování:
$instances = [];
foreach ($instancesList as $instId => $instAttrs) {     // $instId = "1", "2", ... (typ STRING - vyžaduje to konfigurační JSON v KBC)
    if (!empty($processedInstances[$instId])) {         // vstupní hodnota false se vyhodnotí jako empty :)
        $instances[(int)$instId] = $instAttrs;          // (int) ... výstupní pole $instances má klíče číslované typem INT
    }    
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// struktura tabulek

/* základní požadavky nutné u pořadí tabulek:
        - 'records' a 'recordSnapshots' se odkazují na 'statuses'.'idstatus' → musí být uvedeny až za 'statuses' (pro případ použití commonStatuses)
        - 'records' a 'fieldValues' se tvoří pomocí pole $fields vzniklého z tabulky 'fields' → musí být uvedeny až za 'fields' (kvůli foreach)
   detailní požadavky pořadí tabulek (respektující integritní vazby mezi tabulkami pro správnou funkci integritní validace - stejné jako u writeru):
        skupina 1  -  (groups)*, (instances)*, statuses                         * - out-only tabulky, vznikají v transformaci
        skupina 2  -  queues, fields, users, pauses
        skupina 3  -  loginSessions, pauseSessions, queueSessions, calls
        skupina 4  -  records
        skupina 5  -  recordSnapshots, (fieldValues)*
        skupina 6  -  databases, ticketSla, crmRecordTypes
        skupina 7  -  accounts, ticketCategories
        skupina 8  -  contacts
        skupina 9  -  tickets
        skupina 10 -  crmRecords, activities
        skupina 11 -  crmRecordSnapshots
*/

// vstupně-výstupní tabulky (načtou se jako vstupy, transformují se a výsledek je zapsán jako výstup)

// "tab" => ["instPrf" - prefixovat hodnoty ve sloupci identifikátorem instance (0/1), "pk" - primární klíč (0/1), "fk" - cizí klíč (tabName),
//           "json" - jen rozparsovat / rozparsovat a pokračovat ve zpracování hodnoty (0/1), "ti" - parametr pro časovou restrikci záznamů]

$tabsInOutV56_part1 = [
    // skupina 1 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    "statuses"          =>  [   "idstatus"              =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0]
                            ],
    // skupina 2 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    "queues"            =>  [   "idqueue"               =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],   // "fk" => "instances" neuvedeno (u instancí není třeba integritní validace)
                                "idgroup"               =>  ["instPrf" => 0]
                            ],  // 'idgroup' je v IN tabulce NÁZEV → neprefixovat;  není to FK podléhající integritní validaci (groups jsou udvozeny z queues)
    "fields"            =>  [   "idfield"               =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],
                                "name"                  =>  ["instPrf" => 0]
                            ],
    "users"             =>  [   "iduser"                =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],
                                "email"                 =>  ["instPrf" => 0]
                            ],
    "pauses"            =>  [   "idpause"               =>  ["instPrf" => 1, "pk" => 1],
                                "title"                 =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],
                                "type"                  =>  ["instPrf" => 0],
                                "paid"                  =>  ["instPrf" => 0]
                            ],
    // skupina 3 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    "loginSessions"     =>  [   "idloginsession"        =>  ["instPrf" => 1, "pk" => 1],
                                "start_time"            =>  ["instPrf" => 0, "ti" => 1],
                                "end_time"              =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"]
                            ],
    "pauseSessions"     =>  [   "idpausesession"        =>  ["instPrf" => 1, "pk" => 1],
                                "start_time"            =>  ["instPrf" => 0, "ti" => 1],
                                "end_time"              =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "idpause"               =>  ["instPrf" => 0, "fk" => "pauses"],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"]
                            ],
    "queueSessions"     =>  [   "idqueuesession"        =>  ["instPrf" => 1, "pk" => 1],
                                "start_time"            =>  ["instPrf" => 0, "ti" => 1], 
                                "end_time"              =>  ["instPrf" => 0],
                                "duration"              =>  ["instPrf" => 0],
                                "idqueue"               =>  ["instPrf" => 1, "fk" => "queues"],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"]
                            ]
];
$tabsInOutV5  = [
    // skupina 3 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    "calls"             =>  [   "idcall"                =>  ["instPrf" => 1, "pk" => 1],
                                "call_time"             =>  ["instPrf" => 0, "ti" => 1],
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
                                "idinstance"            =>  ["instPrf" => 0]
                            ] 
];
$tabsInOutV56_part2 = [
    // skupina 4 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    "records"           =>  [   "idrecord"              =>  ["instPrf" => 1, "pk" => 1],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"],
                                "idqueue"               =>  ["instPrf" => 1, "fk" => "queues"],
                                "idstatus"              =>  ["instPrf" => 1, "fk" => "statuses"],
                                "iddatabase"            =>  ["instPrf" => 1, "fk" => "databases"],
                                "number"                =>  ["instPrf" => 0],
                                "idcall"                =>  ["instPrf" => 1],
                                "action"                =>  ["instPrf" => 0],
                                "edited"                =>  ["instPrf" => 0, "ti" => 1],
                                "created"               =>  ["instPrf" => 0],
                                "idinstance"            =>  ["instPrf" => 0],
                                "form"                  =>  ["instPrf" => 0, "json" => 0]               // "json" => <0/1> ~ jen rozparsovat / rozparsovat a pokračovat ve zpracování hodnoty
                            ],
    // skupina 5 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    "recordSnapshots"   =>  [   "idrecordsnapshot"      =>  ["instPrf" => 1, "pk" => 1],
                                "iduser"                =>  ["instPrf" => 1, "fk" => "users"],
                                "idrecord"              =>  ["instPrf" => 1, "fk" => "records"],
                                "idstatus"              =>  ["instPrf" => 1, "fk" => "statuses"],
                                "idcall"                =>  ["instPrf" => 1],
                                "created"               =>  ["instPrf" => 0, "ti" => 1],
                                "created_by"            =>  ["instPrf" => 1],                           // neuvažujeme jako FK do "users" (není to tak v GD)
                                "nextcall"              =>  ["instPrf" => 0]
                            ]
];
$tabsInOutV6 = [            // vstupně-výstupní tabulky používané pouze u Daktely v6
    // skupina 6 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
    "crmRecordTypes"    =>  [   "idcrmrecordtype"       => ["instPrf" => 1, "pk" => 1],
                                "name"                  => ["instPrf" => 0],
                                "title"                 => ["instPrf" => 0],
                                "description"           => ["instPrf" => 0],
                                "deleted"               => ["instPrf" => 0],
                                "created"               => ["instPrf" => 0, "ti" => 1],
                                "idinstance"            => ["instPrf" => 0]
                            ],
    // skupina 7 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
    // skupina 8 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
                                "form"                  => ["instPrf" => 0, "json" => 1],
                                "number"                => ["instPrf" => 0]
                            ],
    // skupina 9 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
                                "edited"                => ["instPrf" => 0, "ti" => 1],
                                "edited_by"             => ["instPrf" => 1],
                                "first_answer"          => ["instPrf" => 0],
                                "first_answer_duration" => ["instPrf" => 0],
                                "closed"                => ["instPrf" => 0],
                                "unread"                => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0],
                                "form"                  => ["instPrf" => 0, "json" => 0]
                            ],
    // skupina 10 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
                                "edited"                => ["instPrf" => 0, "ti" => 1],
                                "created"               => ["instPrf" => 0],
                                "stage"                 => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0],
                                "form"                  => ["instPrf" => 0, "json" => 0]
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
                                "time"                  => ["instPrf" => 0, "ti" => 1],
                                "time_wait"             => ["instPrf" => 0],
                                "time_open"             => ["instPrf" => 0],
                                "time_close"            => ["instPrf" => 0],
                                "created_by"            => ["instPrf" => 1],
                                "idinstance"            => ["instPrf" => 0],
                                "item"                  => ["instPrf" => 0]
                            ],
    // skupina 11 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
                                "time"                  => ["instPrf" => 0, "ti" => 1],
                                "stage"                 => ["instPrf" => 0],
                                "idinstance"            => ["instPrf" => 0]
                            ]
];
$tabsInOut = [
    5                   =>  array_merge($tabsInOutV56_part1, $tabsInOutV5, $tabsInOutV56_part2),
    6                   =>  array_merge($tabsInOutV56_part1, $tabsInOutV56_part2, $tabsInOutV6)
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
$tabsList_InOut = [
    5                   =>  array_keys($tabsInOut[5]),
    6                   =>  array_keys($tabsInOut[6])
];
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
        $colNames = array_key_exists($tab, $colsInOnly) ? array_diff(array_keys($cols), $colsInOnly[$tab]) : array_keys($cols); // jsou-li některé sloupce jen vstupní nezapočtou se
        if (!array_key_exists($tab, $outTabsColsCount)) {
            $outTabsColsCount[$tab] = count($colNames);
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
$fakeTitle   = "";                                          // původně "(empty value)"
$tabsFakeRow = $tabsList_InOut[5] + $tabsList_InOut[6];     // = všechny InOut tabulky (sjednocení) napříč verzemi (původně jen ["users", "statuses"])

// počty číslic, na které jsou doplňovány ID's (kvůli řazení v GoodData je výhodné mít konst. délku ID's) a oddělovač prefixu od hodnoty
$idFormat = [
    "sep"       =>  "",                                     // znak oddělující ID instance od inkrementálního ID dané tabulky ("", "-" apod.)
    "instId"    =>  ceil(log10(max(2, count($instances)))), // počet číslic, na které je doplňováno ID instance (hodnota před oddělovačem) - určuje se dle počtu instancí
    "idTab"     =>  8,                                      // výchozí počet číslic, na které je doplňováno inkrementální ID dané tabulky (hodnota za oddělovačem);
                                                            // příznakem potvrzujícím, že hodnota dostačovala k indexaci záznamů u všech tabulek, je proměnná $idFormatIdEnoughDigits;
                                                            // nedoplňovat = "" / 0 / NULL / []  (~ hodnota, kterou lze vyhodnotit jako empty)    
    "idField"   =>  3                                       // výchozí počet číslic, na které je doplňováno inkrementální ID hodnot konkrétního form. pole
];

// počet hodnot PK vypsaných na ukázku do logu (při zapnuté integritní validaci)
$pkSampleCount = 15;

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