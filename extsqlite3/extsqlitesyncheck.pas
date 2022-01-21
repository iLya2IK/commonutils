{
 ExtSqliteSynCheck:
   Sqlite Statements Syntax Parser

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ExtSqliteSynCheck;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,
  ECommonObjs, OGLFastList, OGLFastNumList,
  OGLRegExprWrapper,
  ExtSqliteTokens;

type
  TSqliteStmt = (stmtUnknown,
                 stmtComment,      //v
                 stmtPragma,
                 stmtRelease,      //v
                 stmtRollback,     //v
                 stmtSavepoint,    //v
                 stmtCommit,       //v
                 stmtBegin,        //v
                 stmtSelect,       //v
                 stmtInsert,       //v
                 stmtUpdate,       //v
                 stmtUpsert,       //v
                 stmtReplace,      //v
                 stmtDelete,       //v
                 stmtAlterTable,   //v
                 stmtCreateTrigger,
                 stmtCreateTable,  //v
                 stmtCreateIndex,  //v
                 stmtCreateView,
                 stmtCreateVtable,
                 stmtDropIndex,    //v
                 stmtDropTable,    //v
                 stmtDropTrigger,  //v
                 stmtDropView,     //v
                 stmtAnalize);

  { TSqliteSynRule }

  TSqliteSynRule = class
  private
    FReadOnly : Boolean;
    FKind : TSqliteStmt;
    FRule : AnsiString;
    FRegExpr : AnsiString;
    FWRegExpr : TRegExprWrapper;
  public
    constructor Create(aKind : TSqliteStmt; const aRule : String;
      aReadOnly : Boolean);
    constructor CreateRegExpr(aKind : TSqliteStmt; const aRule : String;
      aReadOnly : Boolean);
    destructor Destroy; override;
    function Check(const S : String) : Boolean;
    function GenRegExpr : String;
    property ReadOnly : Boolean read FReadOnly;
    property Kind : TSqliteStmt read FKind;
  end;

  { TSqliteSynRules }

  TSqliteSynRules = class
  private
    FExprTree : Array [TSqliteStmt] of TSqliteSynRule;
  public
    constructor Create;
    destructor Destroy; override;

    class function GenerateSynCheckExpr(aExpr : TSqliteExpr) : String;

    procedure AddRule(aName : TSqliteStmt;
                       const aRule: String; aRO: Boolean);
    procedure AddRuleRegExpr(aName : TSqliteStmt;
                       const aRule : String; aRO : Boolean);

    function CheckExpr(aRule : TSqliteStmt;
                       const aExpr : String) : Boolean; overload;
    function CheckExpr(aRule : TSqliteStmt;
                         aExpr : TSqliteExpr) : Boolean; overload;
    function CheckExpr(aRule : TSqliteSynRule;
                         aExpr : TSqliteExpr) : Boolean; overload;
    function FindRuleForExpr(aExpr : TSqliteExpr) : TSqliteSynRule;
  end;

  { TSqliteSynAnalizer }

  TSqliteSynAnalizer = class
  protected
    FErrorToken : TSqliteToken;
    FErrorPar  : String;
    FErrorCode : Integer;
    FSynExpr : TSqliteExpr;
  private
    class procedure pGoNextNonspace(aExpr : TSqliteExpr; var i : Integer;
      var T : TSqliteToken);
    class procedure pChangeStateGoNextNonspace(aExpr : TSqliteExpr; nst : Byte;
      var State : Byte; var i : Integer; var T : TSqliteToken);
    class function pCheckKeyword(T : TSqliteToken; kw : Cardinal) : Boolean;
    class function pCheckStatementEnd(T : TSqliteToken) : Boolean;
    class function pCheckId(T : TSqliteToken; maxlen : Integer) : Boolean;
    function GetErrorPos : Integer;
    function GetErrorStr : String;
    function GetErrorTokenValue : String;
  public
    constructor Create(aExpr : TSqliteExpr);

    class function CheckStmt(Kind : TSqliteStmt; aExpr : TSqliteExpr) : Boolean;
    class function FindRuleForExpr(aExpr : TSqliteExpr) : TSqliteSynRule;
    class function FindRule(const aExpr : String) : TSqliteSynRule;
    class function FindStmtForExpr(aExpr : TSqliteExpr) : TSqliteStmt;
    class function FindStmt(const aExpr : String) : TSqliteStmt;

    property ErrorCode : Integer read FErrorCode;
    property ErrorPos : Integer read GetErrorPos;
    property ErroredToken : TSqliteToken read FErrorToken;
    property ErrorStr : String read GetErrorStr;
    property ErrorTokenValue : String read GetErrorTokenValue;
  end;

function SqliteSynRules : TSqliteSynRules;
procedure SetSqliteSynRules(aRules : TSqliteSynRules);

const SAER_NOERROR              = 0;
      SAER_WAIT_TO_RUN          = -1;
      SAER_SYNEXPR_NOT_ASSIGNED = -10;
      SAER_SYNEXPR_EMPTY        = -20;
      SAER_UNEXPECTED           = -30;
      SAER_NOT_SUPPORTED        = -40;
      SAER_ALREADY_EXISTS       = -50;
      SAER_PK_ONLY_INTEGER      = -60;
      SAER_NO_SUCH_COLUMN       = -70;
      SAER_NO_SUCH_TABLE        = -80;
      SAER_KEYWORD_EXPECTED     = $100;
      SAER_IDENTIFIER_EXPECTED  = $200;
      SAER_SYMBOL_EXPECTED      = $400;
      SAER_VALUE_EXPECTED       = $800;

implementation

uses
  LazUTF8, ExtSqliteUtils;

const cSqliteErrorMsg    = 'Error ''%s'' detected at pos %d ''%s''';
      cSqliteNoErrorMsg  = 'No error';
      cSqlite_SAER_WAIT_TO_RUN          = 'Waiting for run';
      cSqlite_SAER_SYNEXPR_NOT_ASSIGNED = 'Syntax expression not assigned';
      cSqlite_SAER_SYNEXPR_EMPTY        = 'Syntax expression is empty';
      cSqlite_SAER_UNEXPECTED           = 'Unexpected';
      cSqlite_SAER_NOT_SUPPORTED        = 'Not supported';
      cSqlite_SAER_ALREADY_EXISTS       = 'already exists';
      cSqlite_SAER_KEYWORD_EXPECTED     = '%s keyword expected';
      cSqlite_SAER_ANY_KEYWORD_EXPECTED = 'suitable keyword expected';
      cSqlite_SAER_IDENTIFIER_EXPECTED  = 'identifier expected';
      cSqlite_SAER_SYMBOL_EXPECTED      = 'symbol %s expected';
      cSqlite_SAER_ANY_SYMBOL_EXPECTED  = 'suitable symbol expected';
      cSqlite_SAER_VALUE_EXPECTED       = 'suitable value expected' ;
      cSqlite_SAER_PK_ONLY_INTEGER      = 'Primary key is only allowed for integer columns';
      cSqlite_SAER_NO_SUCH_COLUMN       = 'No such column(s) %s';
      cSqlite_SAER_NO_SUCH_TABLE        = 'No such table %s';

var vSynRules : TSqliteSynRules = nil;

function SqliteSynRules : TSqliteSynRules;
begin
  Result := vSynRules;
end;

procedure SetSqliteSynRules(aRules : TSqliteSynRules);
begin
  if Assigned(vSynRules) then vSynRules.Free;
  vSynRules := aRules;
end;

{ TSqliteSynRule }

constructor TSqliteSynRule.Create(aKind : TSqliteStmt; const aRule : String;
  aReadOnly : Boolean);
begin
  FKind := aKind;
  FRule := aRule;
  FWRegExpr := nil;
  FRegExpr := GenRegExpr;
  FReadOnly := aReadOnly;
end;

constructor TSqliteSynRule.CreateRegExpr(aKind : TSqliteStmt;
  const aRule : String; aReadOnly : Boolean);
begin
  FKind := aKind;
  FRule := aRule;
  FWRegExpr := nil;
  FRegExpr := aRule;
  FWRegExpr := TRegExprWrapper.Create(FRegExpr);
  FReadOnly := aReadOnly;
end;

destructor TSqliteSynRule.Destroy;
begin
  if Assigned(FWRegExpr) then FWRegExpr.Free;
  inherited Destroy;
end;

function TSqliteSynRule.Check(const S : String) : Boolean;
begin
  Result := FWRegExpr.Exec(S);
end;

function TSqliteSynRule.GenRegExpr : String;
var L0 : Integer;
begin
  //simple representation for sqlite syn rules
  //   for example:
  //   from rule
  //     CREATE[TEMP,TEMPORARY]TABLE[IF NOT EXISTS]id1-2(...)
  //   we must generate regexpr
  //     ^CREATE(TEMP|TEMPORARY){0,1}TABLE(IFNOTEXISTS){0,1}(id1|id2)\(.+\)($|;)
  Result := rewReplace('\s+', FRule, '', false);
  Result := StringReplace(Result, '(', '\(', [rfReplaceAll]);
  Result := StringReplace(Result, ')', '\)', [rfReplaceAll]);
  Result := StringReplace(Result, '...', '(.+)', [rfReplaceAll]);
  Result := StringReplace(Result, 'id1-2', '(id1|id2)', [rfReplaceAll]);
  Result := rewReplace(',([A-Z]+)', Result, '|$1', true);
  Result := rewReplace('([A-Z]+),', Result, '$1|', true);
  Result := rewReplace('\{([A-Z\|]+)\}', Result, '($1){1}', true);
  repeat
   L0 := Length(Result);
   Result := rewReplace('\[([^\[\]]+)\]', Result, '($1){0,1}', true);
  until Length(Result) = L0;
  Result := rewReplace('\<\<(.+)\>\>', Result, '($1(,){0,1})+', true);
  Result := '^' + Result + '($|;)';

  if assigned(FWRegExpr) then
    FWRegExpr.Free;
  FWRegExpr := TRegExprWrapper.Create(Result);
end;

{ TSqliteSynRules }

constructor TSqliteSynRules.Create;
begin
  inherited Create;
  {$ifdef CPU64}
  FillQWord(FExprTree, Length(FExprTree), Qword(nil));
  {$else}
  FillDWord(FExprTree, Length(FExprTree), Dword(nil));
  {$endif}
end;

destructor TSqliteSynRules.Destroy;
var i : TSqliteStmt;
begin
  for i := Low(TSqliteStmt) to High(TSqliteStmt) do
  if assigned(FExprTree[i]) then
  begin
    FExprTree[i].Free;
  end;
  inherited Destroy;
end;

class function TSqliteSynRules.GenerateSynCheckExpr(aExpr : TSqliteExpr
  ) : String;
var i : integer;
begin
  Result := '';
  for i := 0 to aExpr.Count-1 do
  begin
    case aExpr[i].Kind of
      stkSpace, stkComment : ;
      stkIdentifier : begin
        Result := Result + 'id' + Inttostr(aExpr[i].IdCnt);
      end;
      stkKeyWord :
        Result := Result + aExpr[i].QuotedToken;
      stkString :
        Result := Result + 'str';
      stkNumber :
        Result := Result + 'num';
      stkSymbol :
        Result := Result + aExpr[i].Token;
    end;
  end;
end;

procedure TSqliteSynRules.AddRule(aName : TSqliteStmt; const aRule : String;
  aRO : Boolean);
var R : TSqliteSynRule;
begin
  R := TSqliteSynRule.Create(aName, aRule, aRO);
  if Assigned(FExprTree[aName]) then FExprTree[aName].Free;
  FExprTree[aName] := R;
end;

procedure TSqliteSynRules.AddRuleRegExpr(aName : TSqliteStmt;
  const aRule : String; aRO : Boolean);
var R : TSqliteSynRule;
begin
  R := TSqliteSynRule.CreateRegExpr(aName, aRule, aRO);
  if Assigned(FExprTree[aName]) then FExprTree[aName].Free;
  FExprTree[aName] := R;
end;

function TSqliteSynRules.CheckExpr(aRule : TSqliteStmt; const aExpr : String
  ) : Boolean;
var R : TSqliteSynRule;
    lExpr : TSqliteExpr;
begin
  R := FExprTree[aRule];
  if Assigned(R) then
  begin
    lExpr := TSqliteExpr.Create(aExpr);
    try
      Result := CheckExpr(R, lExpr);
    finally
      lExpr.Free;
    end;
  end else
    Result := false
end;

function TSqliteSynRules.CheckExpr(aRule : TSqliteStmt; aExpr : TSqliteExpr
  ) : Boolean;
var R : TSqliteSynRule;
begin
  R := FExprTree[aRule];
  if Assigned(R) then
    Result := CheckExpr(R, aExpr) else
    Result := false;
end;

function TSqliteSynRules.CheckExpr(aRule : TSqliteSynRule; aExpr : TSqliteExpr
  ) : Boolean;
var
  S : String;
begin
  S := GenerateSynCheckExpr(aExpr);
  Result := aRule.Check(S);
end;

function TSqliteSynRules.FindRuleForExpr(aExpr : TSqliteExpr) : TSqliteSynRule;
var i : TSqliteStmt;
    S : String;
begin
  S := GenerateSynCheckExpr(aExpr);
  for i := Low(TSqliteStmt) to High(TSqliteStmt) do
  if Assigned(FExprTree[i]) then
  begin
    if FExprTree[i].Check(S) then
      Exit(FExprTree[i]);
  end;
  Result := nil;
end;

{ TSqliteSynAnalizer }

function TSqliteSynAnalizer.GetErrorPos : Integer;
begin
  if Assigned(FErrorToken) then
    Result := FErrorToken.Pos else
    Result := 0;
end;

function TSqliteSynAnalizer.GetErrorStr : String;
var S : String;
begin
  if FErrorCode = 0 then
  begin
    Result := cSqliteNoErrorMsg;
  end else begin
    if ErrorCode > 0 then
    begin
      S := '';
      if (ErrorCode and SAER_SYMBOL_EXPECTED) > 0 then
      begin
        if ErrorCode and $ff = 0 then S := cSqlite_SAER_ANY_SYMBOL_EXPECTED else
         S := Format(cSqlite_SAER_SYMBOL_EXPECTED, [String(Chr(ErrorCode and $ff))]);
      end;
      if (ErrorCode and SAER_KEYWORD_EXPECTED) > 0 then
      begin
        if Length(S) > 0 then S := S + ' / ';
        if ErrorCode and $ff = 0 then S := S + cSqlite_SAER_ANY_KEYWORD_EXPECTED else
         S := S + Format(cSqlite_SAER_KEYWORD_EXPECTED, [sqluGetIndexedKeyWord(ErrorCode and $ff-1)]);
      end;
      if (ErrorCode and SAER_IDENTIFIER_EXPECTED) > 0 then
      begin
        if Length(S) > 0 then S := S + ' / ';
        S := S + cSqlite_SAER_IDENTIFIER_EXPECTED;
      end;
      if (ErrorCode and SAER_VALUE_EXPECTED) > 0 then
      begin
        if Length(S) > 0 then S := S + ' / ';
        S := S + cSqlite_SAER_VALUE_EXPECTED;
      end;
    end else
    begin
      if ErrorCode = SAER_WAIT_TO_RUN then S := cSqlite_SAER_WAIT_TO_RUN else
      if ErrorCode = SAER_SYNEXPR_NOT_ASSIGNED then S := cSqlite_SAER_SYNEXPR_NOT_ASSIGNED else
      if ErrorCode = SAER_SYNEXPR_EMPTY then S := cSqlite_SAER_SYNEXPR_EMPTY else
      if ErrorCode = SAER_UNEXPECTED then S := cSqlite_SAER_UNEXPECTED else
      if ErrorCode = SAER_NOT_SUPPORTED then S := cSqlite_SAER_NOT_SUPPORTED else
      if ErrorCode = SAER_ALREADY_EXISTS then S := cSqlite_SAER_ALREADY_EXISTS else
      if ErrorCode = SAER_PK_ONLY_INTEGER then S := cSqlite_SAER_PK_ONLY_INTEGER else
      if ErrorCode = SAER_NO_SUCH_COLUMN then begin
        if Length(FErrorPar) > 0 then
          S := Format(cSqlite_SAER_NO_SUCH_COLUMN, [FErrorPar])
        else
        if Assigned(FErrorToken) then
          S := Format(cSqlite_SAER_NO_SUCH_COLUMN, [FErrorToken.QuotedToken]) else
          S := Format(cSqlite_SAER_NO_SUCH_COLUMN, [''])
      end else
      if ErrorCode = SAER_NO_SUCH_TABLE then begin
        if Length(FErrorPar) > 0 then
          S := Format(cSqlite_SAER_NO_SUCH_TABLE, [FErrorPar])
        else
        if Assigned(FErrorToken) then
          S := Format(cSqlite_SAER_NO_SUCH_TABLE, [FErrorToken.QuotedToken]) else
          S := Format(cSqlite_SAER_NO_SUCH_TABLE, [''])
      end else
      S := '';
    end;
    Result := Format(cSqliteErrorMsg, [S, ErrorPos, ErrorTokenValue]);
  end;
end;

function TSqliteSynAnalizer.GetErrorTokenValue : String;
begin
  if Assigned(ErroredToken) then
    Result := ErroredToken.Token else
    Result := '';
end;

constructor TSqliteSynAnalizer.Create(aExpr : TSqliteExpr);
begin
  FSynExpr := aExpr;
  FErrorCode := SAER_WAIT_TO_RUN;
  FErrorPar := '';
  FErrorToken := nil;
end;

class procedure TSqliteSynAnalizer.pGoNextNonspace(aExpr : TSqliteExpr;
                     var i : Integer; var T : TSqliteToken);
var ind : integer;
begin
  for ind := i+1 to aExpr.count-1 do
  begin
    Inc(i);
    T := aExpr[i];
    if T.Kind <> stkSpace then begin
      Exit;
    end;
  end;
end;

class procedure TSqliteSynAnalizer.pChangeStateGoNextNonspace(
                     aExpr : TSqliteExpr;
                     nst : Byte;
                     var State : Byte;
                     var i : Integer;
                     var T : TSqliteToken);
begin
  State := nst;
  pGoNextNonspace(aExpr, i, T);
end;

class function TSqliteSynAnalizer.pCheckKeyword(T : TSqliteToken;
                     kw : Cardinal) : Boolean;
begin
  Result := (T.Kind = stkKeyWord) and
            (T.KeyWordIndx = kw);
end;

class function TSqliteSynAnalizer.pCheckStatementEnd(T : TSqliteToken
  ) : Boolean;
begin
  Result := (T.Kind = stkSymbol) and
            (T.SymbolOrd = STOK_SEMICOLUMN);
end;

class function TSqliteSynAnalizer.pCheckId(T : TSqliteToken; maxlen : Integer
  ) : Boolean;
begin
  Result := (T.Kind = stkIdentifier) and
            (T.IdCnt <= maxlen);
end;

class function TSqliteSynAnalizer.CheckStmt(Kind : TSqliteStmt;
  aExpr : TSqliteExpr) : Boolean;
begin
  Result := vSynRules.CheckExpr(Kind, aExpr);
end;

class function TSqliteSynAnalizer.FindRuleForExpr(aExpr: TSqliteExpr
  ): TSqliteSynRule;
begin
  Result := vSynRules.FindRuleForExpr(aExpr);
end;

class function TSqliteSynAnalizer.FindRule(const aExpr: String): TSqliteSynRule;
var Ex : TSqliteExpr;
begin
  Ex := TSqliteExpr.Create(aExpr);
  try
    Result := vSynRules.FindRuleForExpr(Ex);
  finally
    Ex.Free;
  end;
end;

class function TSqliteSynAnalizer.FindStmtForExpr(aExpr : TSqliteExpr
  ) : TSqliteStmt;
var R : TSqliteSynRule;
begin
  R := FindRuleForExpr(aExpr);
  if Assigned(R) then Result := R.Kind else
                      Result := stmtUnknown;
end;

class function TSqliteSynAnalizer.FindStmt(const aExpr : String) : TSqliteStmt;
var R : TSqliteSynRule;
begin
  R := FindRule(aExpr);
  if Assigned(R) then Result := R.Kind else
                      Result := stmtUnknown;
end;

initialization
  vSynRules := TSqliteSynRules.Create;
  //todo : move to ext file
  vSynRules.AddRuleRegExpr(stmtComment,
                    '^(--.*)|(\/\*.*?\*\/)$', true);
  vSynRules.AddRule(stmtPragma,
                    'PRAGMA id1-2 ...', true);
  vSynRules.AddRule(stmtRelease,
                    'RELEASE[SAVEPOINT]id1', true);
  vSynRules.AddRule(stmtSavepoint,
                    'SAVEPOINT id1', true);
  vSynRules.AddRule(stmtCommit,
                    '{COMMIT,END}[TRANSACTION]', true);
  vSynRules.AddRule(stmtBegin,
                    'BEGIN[DEFERRED,IMMEDIATE,EXCLUSIVE][TRANSACTION]', true);
  vSynRules.AddRule(stmtRollback,
                    'ROLLBACK[TRANSACTION][TO[SAVEPOINT]id1]', false);
  vSynRules.AddRule(stmtCreateTable,
                    'CREATE[TEMP,TEMPORARY]TABLE[IF NOT EXISTS]id1-2(...)[<<{WITHOUT ROWID,STRICT}>>]', false);
  vSynRules.AddRule(stmtAlterTable,
                    'ALTER TABLE id1-2[RENAME TO,{RENAME,ADD,DROP}[COLUMN]]id1[...]', false);
  vSynRules.AddRule(stmtCreateIndex,
                    'CREATE[UNIQUE]INDEX[IF NOT EXISTS]id1-2 ON id1-2...', false);
  vSynRules.AddRule(stmtSelect,
                    '[WITH[RECURSIVE]<<id1-2...AS[[NOT]MATERIALIZED]({SELECT,VALUES}...)>>]{SELECT,VALUES}...', true);
  vSynRules.AddRule(stmtInsert,
                    '[WITH[RECURSIVE]<<id1-2...AS[[NOT]MATERIALIZED]({SELECT,VALUES}...)>>]INSERT[OR{ABORT,FAIL,IGNORE,REPLACE,ROLLBACK}]INTOid1-2...[DEFAULT]VALUES[...]', false);
  vSynRules.AddRule(stmtReplace,
                    '[WITH[RECURSIVE]<<id1-2...AS[[NOT]MATERIALIZED]({SELECT,VALUES}...)>>]REPLACE INTO id1-2...[[DEFAULT]VALUES,SELECT][...]', false);
  vSynRules.AddRule(stmtUpdate,
                    '[WITH[RECURSIVE]<<id1-2...AS[[NOT]MATERIALIZED]({SELECT,VALUES}...)>>]UPDATE[OR{ABORT,FAIL,IGNORE,REPLACE,ROLLBACK}]id1-2 SET...', false);
  vSynRules.AddRule(stmtUpsert,
                    'ON CONFLICT[...]DO{NOTHING,UPDATE SET}[...]', false);
  vSynRules.AddRule(stmtDelete,
                    '[WITH[RECURSIVE]<<id1-2...AS[[NOT]MATERIALIZED]({SELECT,VALUES}...)>>]DELETE FROM id1-2[...]', false);
  vSynRules.AddRule(stmtDropIndex,
                    'DROP INDEX [IF EXISTS] id1-2', false);
  vSynRules.AddRule(stmtDropTable,
                    'DROP TABLE [IF EXISTS] id1-2', false);
  vSynRules.AddRule(stmtDropTrigger,
                    'DROP TRIGGER [IF EXISTS] id1-2', false);
  vSynRules.AddRule(stmtDropView,
                    'DROP VIEW [IF EXISTS] id1-2', false);

finalization
  if assigned(vSynRules) then
    vSynRules.Free;

end.

