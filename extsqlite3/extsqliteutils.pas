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

  TSqliteDataTypeAffinity = (dtaUnknown, dtaInteger, dtaText,
                             dtaBlob, dtaReal, dtaNumeric);

  function sqluConstraintKindToStr(aKind : TSqliteConstrKind) : String;

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

  {$IFDEF LOAD_DYNAMICALLY}
  procedure InitializeSqlite3ExtFuncs;
  {$ENDIF}
  function sqluGetKeyWordCount : Integer;
  function sqluGetKeyWordName(N : Integer) : String;
  function sqluGetIndexedKeyWordCount() : Integer;
  function sqluGetIndexedKeyWord(Index : Cardinal) : String;
  function sqluCheckKeyWord(const KW : String) : Integer;
  function sqluQuotedId(const S : String) : String;
  function sqluQuotedStr(const S : String) : String;
  function sqluCheckIsNeedQuoted(const Id : String) : Boolean;
  function sqluCompareNames(const S1, S2 : String) : Boolean;


const
  kwCREATE = 0;
  kwTEMP = 1;
  kwTEMPORARY = 2;
  kwTABLE = 3;
  kwIF = 4;
  kwNOT = 5;
  kwEXISTS = 6;
  kwAS = 7;
  kwWITHOUT = 8;
  kwROWID = 9;
  kwCONSTRAINT = 10;
  kwPRIMARY = 11;
  kwUNIQUE = 12;
  kwCHECK = 13;
  kwDEFAULT = 14;
  kwCOLLATE = 15;
  kwREFERENCES = 16;
  kwGENERATED = 17;
  kwKEY = 18;
  kwNULL = 19;
  kwON = 20;
  kwTRUE = 21;
  kwFALSE = 22;
  kwCURRENT_TIME = 23;
  kwCURRENT_DATE = 24;
  kwCURRENT_TIMESTAMP = 25;
  kwALWAYS = 26;
  kwASC = 27;
  kwDESC = 28;
  kwCONFLICT = 29;
  kwSTORED = 30;
  kwVIRTUAL = 31;
  kwAUTOINCREMENT = 32;
  kwROLLBACK = 33;
  kwABORT = 34;
  kwFAIL = 35;
  kwIGNORE = 36;
  kwREPLACE = 37;
  kwMATCH = 38;
  kwDEFERRABLE = 39;
  kwDELETE = 40;
  kwUPDATE = 41;
  kwINITIALLY = 42;
  kwSET = 43;
  kwCASCADE = 44;
  kwRESTRICT = 45;
  kwDEFERRED = 46;
  kwIMMEDIATE = 47;
  kwACTION = 48;
  kwNO = 49;
  kwFOREIGN = 50;

implementation

uses LazUTF8;

const
  sqliteAvaibleKeyWords : Array [0..50] of string =
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
     'ACTION', 'NO', 'FOREIGN');

const
  sqliteAffinity : Array [TSqliteDataTypeAffinity] of string= (
   'UNKNOWN',
   'INT',
   'TEXT',
   'BLOB',
   'REAL',
   'NUMERIC'
  );

const
  sqliteAvaibleDataTypeParts : Array [0..28] of string =
    ('INT',              //0
     'INTEGER',          //1
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

function sqluConstraintKindToStr(aKind : TSqliteConstrKind) : String;
begin
  Result := DBConstrToStr[aKind];
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

function sqluCheckKeyWord(const KW: String): Integer;
begin
  Result := sqlite3_keyword_check(@(KW[1]), Length(KW));
end;

function sqluQuotedId(const S : String) : String;
begin
  Result := '"' + UTF8StringReplace(S, '"', '""', [rfReplaceAll]) + '"';
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

function sqluGetIndexedKeyWordCount() : Integer;
begin
  Result := Length(sqliteAvaibleKeyWords);
end;

function sqluGetIndexedKeyWord(Index : Cardinal) : String;
begin
  Result := sqliteAvaibleKeyWords[Index];
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

end.

