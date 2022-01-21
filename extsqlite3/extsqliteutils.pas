{
 ExtSqliteUtils:
   Additional utilities to use with Sqlite3 lib.

   Part of CommonUtils project

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ExtSqliteUtils;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,
  ECommonObjs,
  AvgLvlTree,
  {$IFDEF LOAD_DYNAMICALLY}
  SQLite3Dyn,
  DynLibs,
  {$ELSE}
  SQLite3,
  {$ENDIF}
  ctypes;

type
  TSqliteConstrKind = (dbckPrimaryKey, dbckNotNull,
                       dbckUnique, dbckCheck,
                       dbckDefault,
                       dbckCollate,
                       dbckForeignKey,
                       dbckGenerated);

  TSqliteValueKind = (dbvkExpression,
                      dbvkDefaultValue,
                      dbvkCollationName,
                      dbvkIndexedColumns,
                      dbvkColumns,
                      dbvkTable);

  TSqliteTableOption = (toCheckExists, toIsTemp, toWORowID);

  TSqliteDataTypeAffinity = (dtaUnknown, dtaInteger, dtaText,
                             dtaBlob, dtaReal, dtaNumeric);

  TSqliteKwFormatOption = (skfoOriginal, skfoUpperCase, skfoLowerCase, skfoFromCapit);

  { TSqliteMemoryDB }

  TSqliteMemoryDB = class(TThreadSafeObject)
  private
    FDB : psqlite3;
    FLastError : TThreadUtf8String;
    function GetLastError : String;
    procedure SetLastError(AValue : String);
  public
    constructor Create;
    function Initialize(const aName : String) : Integer;
    destructor destroy; override;
    property LastError : String read GetLastError write SetLastError;
    property Handle : psqlite3 read FDB;
  end;


  function sqluGetVersionNum : Integer;
  function sqluGetVersionStr : String;

  function sqluConstraintKindToStr(aKind : TSqliteConstrKind) : String;
  function sqluTableOptionKindToStr(aKind : TSqliteTableOption) : String;

  function sqluCheckExpr(const aExpr : String; aDB : TSqliteMemoryDB = nil) : Integer;
  function sqluCheckExpr(const aExpr : String; aDB : psqlite3) : Integer; overload;
  function sqluCheckExprIsReadOnly(const aExpr : String; aDB : psqlite3) : Boolean;
  function sqluGetCheckExprLastError : String;
  function sqluGetLastError(aSqliteHandle : psqlite3; err : Integer) : String;
  function sqluCode2Str(Code: Integer): String;
  function sqluAvaibleDataTypesPartsCount() : Integer;
  function sqluAvaibleDataTypesPart(Pos : Cardinal) : String;
  function sqluGetDataTypeAffinity(const aDataType : String) : TSqliteDataTypeAffinity;
  function sqluAffinityToStr(aff : TSqliteDataTypeAffinity) : String;
  {$IFDEF LOAD_DYNAMICALLY}
    {$DEFINE D}
  {$ELSE}
    {$DEFINE S}
  {$ENDIF}
  {$IFDEF S}function{$ELSE}var{$ENDIF} sqlite3_keyword_count{$IFDEF D}: function{$ENDIF}(): cint;cdecl;{$IFDEF S}external Sqlite3Lib;{$ENDIF}
  {$IFDEF S}function{$ELSE}var{$ENDIF} sqlite3_keyword_name{$IFDEF D}: function{$ENDIF}(n:cint;res:ppansichar;l:pcint):cint;cdecl;{$IFDEF S}external Sqlite3Lib;{$ENDIF}
  {$IFDEF S}function{$ELSE}var{$ENDIF} sqlite3_keyword_check{$IFDEF D}: function{$ENDIF}(expr:pansichar;l:cint):cint;cdecl;{$IFDEF S}external Sqlite3Lib;{$ENDIF}
  {$IFDEF S}function{$ELSE}var{$ENDIF} sqlite3_stricmp{$IFDEF D}: function{$ENDIF}(expr1, expr2:pansichar):cint;cdecl;{$IFDEF S}external Sqlite3Lib;{$ENDIF}
  {$IFDEF S}function{$ELSE}var{$ENDIF} sqlite3_strnicmp{$IFDEF D}: function{$ENDIF}(expr1, expr2:pansichar;l:cint):cint;cdecl;{$IFDEF S}external Sqlite3Lib;{$ENDIF}

  {$IFDEF S}function{$ELSE}var{$ENDIF} sqlite3_stmt_readonly{$IFDEF D}: function{$ENDIF}(pStmt : psqlite3_stmt): cint;cdecl;{$IFDEF S}external Sqlite3Lib;{$ENDIF}

  {$IFDEF LOAD_DYNAMICALLY}
  procedure InitializeSqlite3ExtFuncs;
  {$ENDIF}
  function sqluGetKeyWordCount : Integer;
  function sqluGetKeyWordName(N : Integer) : String;
  function sqluGetIndexedKeyWordCount() : Integer;
  function sqluGetIndexedKeyWordInd(const KW : String) : Integer;
  function sqluGetIndexedKeyWord(Index : Cardinal;
                                   aOption : TSqliteKwFormatOption =
                                                        skfoUpperCase) : String;
  function sqluGetIndexedKeyWords(const Indexes : Array of Cardinal;
                                   aOption : TSqliteKwFormatOption =
                                                        skfoUpperCase) : String;
  function sqluCheckKeyWord(const KW : String) : Integer;
  function sqluFormatKeyWord(const KW : String;
                              aOption : TSqliteKwFormatOption =
                                                        skfoUpperCase) : String;
  function sqluDetectFormat(const aToken : String) : TSqliteKwFormatOption;
  function sqluGetKeyWordsList() : String;
  function sqluGetFunctionsList() : String;
  function sqluGetDataTypesList() : String;

  function sqluQuotedId(const S : String) : String;
  function sqluUnquotedId(const S : String) : String;
  function sqluQuotedStr(const S : String) : String;
  function sqluCheckIsNeedQuoted(const Id : String) : Boolean;
  function sqluQuotedIdIfNeeded(const S : String) : String;
  function sqluUnquotedIdIfNeeded(const S : String) : String;
  function sqluCompareNames(const S1, S2 : String) : Boolean;

  //memory db functions
  function sqluNewMemoryDB(const aName : String) : psqlite3;
  function sqluExecuteInMemory(mem : psqlite3; const aExpr : String) : Cardinal;
  procedure sqluDeleteMemoryDB(mem : psqlite3);

const
  kwCREATE : Word = 0;
  kwTEMP   : Word = 1;
  kwTEMPORARY: Word = 2;
  kwTABLE: Word = 3;
  kwIF: Word = 4;
  kwNOT: Word = 5;
  kwEXISTS: Word = 6;
  kwAS: Word = 7;
  kwWITHOUT: Word = 8;
  kwROWID: Word = 9;
  kwCONSTRAINT: Word = 10;
  kwPRIMARY: Word = 11;
  kwUNIQUE: Word = 12;
  kwCHECK: Word = 13;
  kwDEFAULT: Word = 14;
  kwCOLLATE: Word = 15;
  kwREFERENCES: Word = 16;
  kwGENERATED: Word = 17;
  kwKEY: Word = 18;
  kwNULL: Word = 19;
  kwON: Word = 20;
  kwTRUE: Word = 21;
  kwFALSE: Word = 22;
  kwCURRENT_TIME: Word = 23;
  kwCURRENT_DATE: Word = 24;
  kwCURRENT_TIMESTAMP: Word = 25;
  kwALWAYS: Word = 26;
  kwASC: Word = 27;
  kwDESC: Word = 28;
  kwCONFLICT: Word = 29;
  kwSTORED: Word = 30;
  kwVIRTUAL: Word = 31;
  kwAUTOINCREMENT: Word = 32;
  kwROLLBACK: Word = 33;
  kwABORT: Word = 34;
  kwFAIL: Word = 35;
  kwIGNORE: Word = 36;
  kwREPLACE: Word = 37;
  kwMATCH: Word = 38;
  kwDEFERRABLE: Word = 39;
  kwDELETE: Word = 40;
  kwUPDATE: Word = 41;
  kwINITIALLY: Word = 42;
  kwSET: Word = 43;
  kwCASCADE: Word = 44;
  kwRESTRICT: Word = 45;
  kwDEFERRED: Word = 46;
  kwIMMEDIATE: Word = 47;
  kwACTION: Word = 48;
  kwNO: Word = 49;
  kwFOREIGN: Word = 50;
  kwBEGIN: Word = 51;
  kwTRANSACTION: Word = 52;
  kwEXCLUSIVE: Word = 53;
  kwCOMMIT: Word = 54;
  kwEND: Word = 55;
  kwSAVEPOINT: Word = 56;
  kwRELEASE: Word = 57;
  kwTO: Word = 58;
  kwALTER : Word = 59;
  kwRENAME : Word = 60;
  kwADD : Word = 61;
  kwDROP : Word = 62;
  kwPRAGMA : Word = 63;
  kwINSERT : Word = 64;
  kwSELECT : Word = 65;
  kwINTO   : Word = 66;
  kwFROM   : Word = 67;
  kwOR     : Word = 68;
  kwVALUES : Word = 69;

implementation

uses LazUTF8;

const cMaxIndexedKeyWords = 69;

const
  sqliteAvaibleKeyWords : Array [0..cMaxIndexedKeyWords] of string =
    ('CREATE', 'TEMP', 'TEMPORARY', 'TABLE', 'IF', 'NOT',
     'EXISTS', 'AS', 'WITHOUT', 'ROWID', 'CONSTRAINT',
     'PRIMARY', 'UNIQUE', 'CHECK', 'DEFAULT', 'COLLATE',
     'REFERENCES', 'GENERATED', 'KEY', 'NULL', 'ON',
     'TRUE', 'FALSE', 'CURRENT_TIME', 'CURRENT_DATE',
     'CURRENT_TIMESTAMP', 'ALWAYS', 'ASC', 'DESC',
     'CONFLICT', 'STORED', 'VIRTUAL', 'AUTOINCREMENT',
     'ROLLBACK', 'ABORT', 'FAIL', 'IGNORE', 'REPLACE',
     'MATCH', 'DEFERRABLE', 'DELETE', 'UPDATE', 'INITIALLY',
     'SET', 'CASCADE', 'RESTRICT', 'DEFERRED', 'IMMEDIATE',
     'ACTION', 'NO', 'FOREIGN', 'BEGIN', 'TRANSACTION',
     'EXCLUSIVE', 'COMMIT', 'END', 'SAVEPOINT', 'RELEASE', 'TO',
     'ALTER', 'RENAME', 'ADD', 'DROP', 'PRAGMA', 'INSERT', 'SELECT',
     'INTO', 'FROM', 'OR', 'VALUES');

const
  sqliteAffinity : Array [TSqliteDataTypeAffinity] of string= (
   'UNKNOWN',
   'INTEGER',
   'TEXT',
   'BLOB',
   'REAL',
   'NUMERIC'
  );

const
  sqliteAvaibleDataTypeParts : Array [0..28] of string =
    ('INTEGER',          //0
     'INT',              //1
     'TINYINT',          //2
     'SMALLINT',         //3
     'MEDIUMINT',        //4
     'BIGINT',           //5
     'UNSIGNED',         //6
          'BIG',// INT', //7
     'INT2',             //8
     'INT8',             //9
     'CHARACTER',//(20) //10
     'VARCHAR',//(255)  //11
     'VARYING',         //12
          'CHARACTER',//(255)//13
     'NCHAR',//(55)     //14
     'NATIVE',          //15
         // CHARACTER',//(70)
     'NVARCHAR',//(100) //16
     'TEXT',            //17
     'CLOB',            //18
     'BLOB',            //19
     'REAL',            //20
     'DOUBLE',          //21
          'PRECISION',  //22
     'FLOAT',           //23
     'NUMERIC',         //24
     'DECIMAL',//(x,x)  //25
     'BOOLEAN',         //26
     'DATE',            //27
     'DATETIME');       //28

const DBConstrToStr : Array [TSqliteConstrKind] of String = (
                   'PrimaryKey',
                   'NotNull',
                   'Unique', 'Check',
                   'Default',
                   'Collate',
                   'ForeignKey',
                   'Generated');

var vSqliteCheckerDB : TSqliteMemoryDB = nil;
    vIndexedStrs     : TStringToPointerTree;

procedure DestroyExprChecker();
begin
  if Assigned(vSqliteCheckerDB) then
    vSqliteCheckerDB.Free;
end;

function sqluGetVersionNum : Integer;
begin
  Result := sqlite3_libversion_number();
end;

function sqluGetVersionStr : String;
begin
  Result := StrPas(sqlite3_libversion());
end;

function sqluConstraintKindToStr(aKind : TSqliteConstrKind) : String;
begin
  Result := DBConstrToStr[aKind];
end;

function sqluTableOptionKindToStr(aKind : TSqliteTableOption) : String;
begin
  case aKind of
    toCheckExists : Result := sqluGetIndexedKeyWords([kwIF, kwNOT, kwEXISTS]);
    toWORowID : Result := sqluGetIndexedKeyWords([kwWITHOUT, kwROWID]);
    toIsTemp : Result := sqluGetIndexedKeyWord(kwTEMP);
  else
    Result := '';
  end;
end;

function sqluCheckExpr(const aExpr : String; aDB : TSqliteMemoryDB = nil) : Integer;
begin
  if not Assigned(aDB) then
  begin
    if not Assigned(vSqliteCheckerDB) then
    begin
      vSqliteCheckerDB := TSqliteMemoryDB.Create;
      vSqliteCheckerDB.Initialize('checker');
    end;
    aDB := vSqliteCheckerDB;
  end;

  aDb.Lock;
  try
    if Assigned(aDb.Handle) then
    begin
      Result := sqluCheckExpr(aExpr, aDB.Handle);
      if Result <> SQLITE_OK then
      begin
        aDb.LastError := sqluCode2Str(Result) + ' - ' +
                                       sqlite3_errmsg(aDB.Handle);
      end else aDb.LastError := '';
    end else Result := SQLITE_MISUSE;
  finally
    aDb.UnLock;
  end;
end;

function sqluCheckExpr(const aExpr : String; aDB : psqlite3) : Integer;
var vm : psqlite3_stmt;
begin
  if Assigned(aDb) then
  begin
    Result := sqlite3_prepare_v2(aDb, PAnsiChar(aExpr), -1, @vm, nil);
    sqlite3_finalize(vm);
  end;
end;

function sqluCheckExprIsReadOnly(const aExpr: String; aDB: psqlite3): Boolean;
var vm : psqlite3_stmt;
    er : Integer;
begin
  if Assigned(aDb) then
  begin
    er := sqlite3_prepare_v2(aDb, PAnsiChar(aExpr), -1, @vm, nil);
    if er = SQLITE_OK then
      Result := sqlite3_stmt_readonly(vm) <> 0 else
      Result := false;
    sqlite3_finalize(vm);
  end;
end;

function sqluGetCheckExprLastError : String;
begin
  if Assigned(vSqliteCheckerDB) then
    Result := vSqliteCheckerDB.LastError else
    Result := '';
end;

function sqluGetLastError(aSqliteHandle : psqlite3; err : Integer) : String;
begin
  Result := sqluCode2Str(err);
  if err <> SQLITE_OK then
    Result := Result + ' - ' + sqlite3_errmsg(aSqliteHandle);
end;

function sqluCode2Str(Code: Integer): String;
begin
  case Code of
    SQLITE_OK           : Result := 'SQLITE_OK';
    SQLITE_ERROR        : Result := 'SQLITE_ERROR';
    SQLITE_INTERNAL     : Result := 'SQLITE_INTERNAL';
    SQLITE_PERM         : Result := 'SQLITE_PERM';
    SQLITE_ABORT        : Result := 'SQLITE_ABORT';
    SQLITE_BUSY         : Result := 'SQLITE_BUSY';
    SQLITE_LOCKED       : Result := 'SQLITE_LOCKED';
    SQLITE_NOMEM        : Result := 'SQLITE_NOMEM';
    SQLITE_READONLY     : Result := 'SQLITE_READONLY';
    SQLITE_INTERRUPT    : Result := 'SQLITE_INTERRUPT';
    SQLITE_IOERR        : Result := 'SQLITE_IOERR';
    SQLITE_CORRUPT      : Result := 'SQLITE_CORRUPT';
    SQLITE_NOTFOUND     : Result := 'SQLITE_NOTFOUND';
    SQLITE_FULL         : Result := 'SQLITE_FULL';
    SQLITE_CANTOPEN     : Result := 'SQLITE_CANTOPEN';
    SQLITE_PROTOCOL     : Result := 'SQLITE_PROTOCOL';
    SQLITE_EMPTY        : Result := 'SQLITE_EMPTY';
    SQLITE_SCHEMA       : Result := 'SQLITE_SCHEMA';
    SQLITE_TOOBIG       : Result := 'SQLITE_TOOBIG';
    SQLITE_CONSTRAINT   : Result := 'SQLITE_CONSTRAINT';
    SQLITE_MISMATCH     : Result := 'SQLITE_MISMATCH';
    SQLITE_MISUSE       : Result := 'SQLITE_MISUSE';
    SQLITE_NOLFS        : Result := 'SQLITE_NOLFS';
    SQLITE_AUTH         : Result := 'SQLITE_AUTH';
    SQLITE_FORMAT       : Result := 'SQLITE_FORMAT';
    SQLITE_RANGE        : Result := 'SQLITE_RANGE';
    SQLITE_ROW          : Result := 'SQLITE_ROW';
    SQLITE_NOTADB       : Result := 'SQLITE_NOTADB';
    SQLITE_DONE         : Result := 'SQLITE_DONE';
  else
    Result := 'Unknown Return Value';
  end;
end;

{$ifdef prepared_multi_holders}
const DEFAULT_THREAD_HOLDER_CNT = 1;
{$endif}

{$IFDEF LOAD_DYNAMICALLY}
procedure InitializeSqlite3ExtFuncs;
begin
  pointer(sqlite3_keyword_count) := GetProcedureAddress(LibHandle,'sqlite3_keyword_count');
  pointer(sqlite3_keyword_name) := GetProcedureAddress(LibHandle,'sqlite3_keyword_name');
  pointer(sqlite3_keyword_check) := GetProcedureAddress(LibHandle,'sqlite3_keyword_check');
  pointer(sqlite3_strnicmp) := GetProcedureAddress(LibHandle,'sqlite3_strnicmp');
  pointer(sqlite3_stricmp) := GetProcedureAddress(LibHandle,'sqlite3_stricmp');
  pointer(sqlite3_stmt_readonly) := GetProcedureAddress(LibHandle,'sqlite3_stmt_readonly');
end;
{$ENDIF}

function sqluGetKeyWordCount: Integer;
begin
  result := sqlite3_keyword_count;
end;

function sqluGetKeyWordName(N : Integer): String;
var p : PAnsiChar;
    L : Integer;
begin
  L := 0;
  if (sqlite3_keyword_name(N, @p, @L) = SQLITE_OK) and
     (L > 0) then
  begin
    SetLength(Result, L);
    Move(p^, Result[1], L);
  end else
    Result := '';
end;

function sqluGetIndexedKeyWords(const Indexes : array of Cardinal;
  aOption : TSqliteKwFormatOption) : String;
var i : integer;
begin
  Result := '';
  for i := 0 to high(Indexes) do
  begin
    if i > 0 then Result := Result + ' ';
    Result := Result + sqluGetIndexedKeyWord(Indexes[i], aOption);
  end;
end;

function sqluCheckKeyWord(const KW: String): Integer;
begin
  Result := sqlite3_keyword_check(@(KW[1]), Length(KW));
  if Result = 0 then begin
    if SameStr(UpperCase(KW), sqliteAvaibleKeyWords[kwROWID]) then
      Result := $0f01;
  end;
end;

function sqluFormatKeyWord(const KW : String; aOption : TSqliteKwFormatOption
  ) : String;
begin
  case aOption of
    skfoOriginal  : Result := KW;
    skfoLowerCase : Result := LowerCase(KW);
    skfoUpperCase : Result := UpperCase(KW);
    skfoFromCapit : Result := UpperCase(Copy(KW, 1, 1)) +
                              LowerCase(Copy(KW, 2, Length(KW)-1));
  end;
end;

function sqluDetectFormat(const aToken : String) : TSqliteKwFormatOption;
begin
  if SameStr(LowerCase(aToken), aToken) then
  begin
    Result := skfoLowerCase;
  end else
  if SameStr(UpperCase(aToken), aToken) then
  begin
    Result := skfoUpperCase;
  end else
    Result := skfoFromCapit;
end;

function sqluGetKeyWordsList() : String;
var i : integer;
    SL : TStringList;
begin
  SL := TStringList.Create;
  try
    SL.Sorted := true;
    SL.Duplicates := dupIgnore;
    for i := 1 to sqluGetKeyWordCount do
      SL.Add(sqluGetKeyWordName(i));
    SL.Add(sqliteAvaibleKeyWords[kwROWID]);
    SL.Delimiter := ',';
    Result := LowerCase(SL.DelimitedText);
  finally
    SL.Free;
  end;
end;

function sqluGetFunctionsList() : String;
begin
  Result := 'abs,avg,changes,coalesce,count,group_concat,hex,iif,ifnull,' +
    'julianday,last_insert_rowid,length,load_extension,lower,ltrim,max,min,' +
    'nullif,quote,random,randomblob,round,rtrim,soundex,sqlite_compileoption_get,' +
    'sqlite_compileoption_used,sqlite_source_id,sqlite_version,strftim,substr,sum,time,' +
    'total,total_changes,trim,typeof,upper,zeroblob';
end;

function sqluGetDataTypesList() : String;
var i : integer;
    SL : TStringList;
begin
  SL := TStringList.Create;
  try
    SL.Sorted := true;
    SL.Duplicates := dupIgnore;
    for i := Low(sqliteAvaibleDataTypeParts) to
             High(sqliteAvaibleDataTypeParts) do
    if not (i in [6, 7, 12, 13, 15, 22]) then
      SL.Add(sqliteAvaibleDataTypeParts[i]);
    SL.Delimiter := ',';
    Result := LowerCase(SL.DelimitedText);
  finally
    SL.Free;
  end;
end;

function sqluQuotedId(const S : String) : String;
begin
  Result := '"' + UTF8StringReplace(S, '"', '""', [rfReplaceAll]) + '"';
end;

function sqluQuotedIdIfNeeded(const S : String) : String;
begin
  if sqluCheckIsNeedQuoted(s) then
    Result := '"' + UTF8StringReplace(S, '"', '""', [rfReplaceAll]) + '"' else
    Result := S;
end;

function sqluUnquotedId(const S : String) : String;
begin
  Result := UTF8Copy(S, 2, UTF8Length(S) - 2);
  Result := UTF8StringReplace(Result, '""', '"', [rfReplaceAll]);
end;

function sqluQuotedStr(const S : String) : String;
begin
  Result := '''' + UTF8StringReplace(S, '''', '''''', [rfReplaceAll]) + '''';
end;

function sqluCheckIsNeedQuoted(const Id : String) : Boolean;
var l, len : Integer;
    chr : PAnsiChar;
begin
  len := Length(Id);
  if len = 0 then Exit(true);

  chr := @(Id[1]);
  l := UTF8CodepointSizeFast(chr);
  if l > 1 then Exit(True);
  if not (chr^ in ['_', 'a'..'z', 'A'..'Z']) then  Exit(True);

  Dec(len);
  while len > 0 do
  begin
    Inc(chr);
    l := UTF8CodepointSizeFast(chr);
    if l > 1 then Exit(True);
    if not (chr^ in ['_', 'a'..'z', 'A'..'Z', '0'..'9']) then Exit(True);
    Dec(len);
  end;
  Result := false;
end;

{.$ifdef d}
function InternalSqliteCompare(const S1, S2 : String) : Boolean;
var
  len, l1, l2: Integer;
  c1, c2 : PAnsiChar;
begin
  len := Length(S1);
  if len <> Length(S2) then Exit(false);
  if len = 0 then Exit(True);
  c1 := PAnsiChar(@(S1[1]));
  c2 := PAnsiChar(@(S2[1]));
  while len > 0 do
  begin
    l1 := UTF8CodepointSizeFast(c1);
    l2 := UTF8CodepointSizeFast(c2);
    if l1 <> l2 then Exit(false);
    case l1 of
      1 : if LowerCase(C1^) <> LowerCase(C2^) then Exit(False);
      2 : if PWord(C1)^ <> PWord(C2)^ then Exit(False);
      3 : if PWord(C1)^ <> PWord(C2)^ then Exit(False) else
          begin
            if (C1 + 2)^ <> (C2 + 2)^ then Exit(False);
          end;
      4 : if PDWord(C1)^ <> PDWord(C2)^ then Exit(False);
    end;
    Inc(C1, l1);
    Inc(C2, l1);
    Dec(len, l1)
  end;
end;
{.$endif}

function sqluUnquotedIdIfNeeded(const S : String) : String;
begin
  if Length(S) > 2 then
  begin
    if S[1] = '"' then begin
      Result := sqluUnquotedId(S);
    end else
      Result := S;
  end else
    Result := S;
end;

function sqluCompareNames(const S1, S2 : String) : Boolean;
var len : integer;
begin
  len := UTF8Length(S1);
  if len <> UTF8Length(S2) then Exit(false);
  {$ifdef s}
  Result := sqlite3_strnicmp(@(S1[1]), @(S2[1]), len) = 0;
  {$else}
  if assigned(sqlite3_strnicmp) then
  begin
    Result := sqlite3_strnicmp(@(S1[1]), @(S2[1]), len) = 0;
  end else
    Result := InternalSqliteCompare(S1, S2);
  {$endif}
end;

function sqluNewMemoryDB(const aName : String) : psqlite3;
var code : Cardinal;
begin
  code := sqlite3_open_v2(PAnsiChar(aName), @Result,
                                             SQLITE_OPEN_CREATE or
                                             SQLITE_OPEN_READWRITE or
                                             SQLITE_OPEN_MEMORY, nil);
  if code <> SQLITE_OK then Result := nil;
end;

function sqluExecuteInMemory(mem : psqlite3; const aExpr : String) : Cardinal;
begin
  Result := sqlite3_exec(mem, PAnsiChar(aExpr), nil, nil, nil);
end;

procedure sqluDeleteMemoryDB(mem : psqlite3);
begin
  sqlite3_close_v2(mem);
end;

function sqluGetIndexedKeyWordCount() : Integer;
begin
  Result := Length(sqliteAvaibleKeyWords);
end;

function sqluGetIndexedKeyWordInd(const KW : String) : Integer;
var
  i : PWord;
begin
  i := PWord(vIndexedStrs.Values[UpperCase(KW)]);
  if assigned(i) then
    Result := i^
  else
    Result := -1;
end;

function sqluGetIndexedKeyWord(Index : Cardinal; aOption : TSqliteKwFormatOption
  ) : String;
begin
  case aOption of
    skfoUpperCase : Result := sqliteAvaibleKeyWords[Index];
  else
    Result := sqluFormatKeyWord(sqliteAvaibleKeyWords[Index], aOption);
  end;
end;

function sqluAvaibleDataTypesPartsCount() : Integer;
begin
  Result := Length(sqliteAvaibleDataTypeParts);
end;

function sqluAvaibleDataTypesPart(Pos : Cardinal) : String;
begin
  Result := sqliteAvaibleDataTypeParts[Pos];
end;

function sqluGetDataTypeAffinity(const aDataType : String
  ) : TSqliteDataTypeAffinity;
begin
  if Pos('INT', aDataType) > 0 then
    Result := dtaInteger else
  if (Pos('CHAR', aDataType) > 0) or
     (Pos('CLOB', aDataType) > 0) or
     (Pos('TEXT', aDataType) > 0) then
    Result := dtaText else
  if (Pos('BLOB', aDataType) > 0) or (Length(aDataType) = 0) then
    Result := dtaBlob else
  if (Pos('REAL', aDataType) > 0) or
     (Pos('FLOA', aDataType) > 0) or
     (Pos('DOUB', aDataType) > 0) then
    Result := dtaReal else
    Result := dtaNumeric;
end;

function sqluAffinityToStr(aff : TSqliteDataTypeAffinity) : String;
begin
  Result := sqliteAffinity[aff];
end;

{ TSqliteMemoryDB }

function TSqliteMemoryDB.GetLastError : String;
begin
  Result := FLastError.Value;
end;

procedure TSqliteMemoryDB.SetLastError(AValue : String);
begin
  FLastError.Value := AValue;
end;

constructor TSqliteMemoryDB.Create;
begin
  inherited Create;
  FLastError := TThreadUtf8String.Create('');
  FDB := nil;
end;

function TSqliteMemoryDB.Initialize(const aName : String) : Integer;
begin
  Result := sqlite3_open_v2(PAnsiChar(aName), @FDB,
                                             SQLITE_OPEN_CREATE or
                                                SQLITE_OPEN_READWRITE or
                                                SQLITE_OPEN_MEMORY,
                                             nil);
  if Result <> SQLITE_OK then FDB := nil;
end;

destructor TSqliteMemoryDB.destroy;
begin
  if Assigned(FDB) then
    sqlite3_close_v2(FDB);
  FDB := nil;
  FLastError.Free;
  inherited destroy;
end;

var i : Word;
    gArray : Array [0..cMaxIndexedKeyWords] of Word;

initialization
  vIndexedStrs := TStringToPointerTree.Create(true);
  vIndexedStrs.FreeValues := false;
  for i := 0 to cMaxIndexedKeyWords do
  begin
    gArray[i] := i;
    vIndexedStrs.Values[sqliteAvaibleKeyWords[i]] := @(gArray[i]);
  end;


finalization
  DestroyExprChecker;
  vIndexedStrs.Free;

end.

