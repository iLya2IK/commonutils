unit OGLRegExprWrapper;

{$mode objfpc}{$H+}
{.$define regexpr_new}
{.$define regexpr_t}
{$ifdef regexpr_new}{$define regexpr_nt}{$endif}
{$ifdef regexpr_t}{$define regexpr_nt}{$endif}

interface

uses
  Classes, SysUtils,
  {$ifdef regexpr_new}
  RegExpr
  {$else}
  {$ifdef regexpr_t}
  regexpr_t
  {$else}
  uregexpr
  {$endif}
  {$endif};

type

  { TRegExprWrapper }

  TRegExprWrapper = class
  private
    rexp : TRegExpr;
    function GetMatch(index : integer): String;
    function GetMatchLen(index : integer): integer;
    function GetMatchPos(index : integer): integer;
  public
    constructor Create(const Expr : String);

    function Exec(const Expr : String) : Boolean;
    function ExecNext : Boolean;
    property Match[index : integer] : String read GetMatch;
    property MatchPos[index : integer] : integer read GetMatchPos;
    property MatchLen[index : integer] : integer read GetMatchLen;
    function SubExprMatchCount : Integer;
    procedure SetModifierR;
    procedure SetModifierM;
    procedure SetModifierI;
    procedure SetModifierG;

    class function GetRusSmallLettersRange : String;

    destructor Destroy; override;
  end;

function rewRegExpr(const aInp, aExpr : String) : Boolean;
function rewReplace(const ARegExpr, AInputStr, AReplaceStr: String;
                                             AUseSubstitution: boolean): String;
function rewRegExprVersionMajor : Integer;
function rewRegExprVersionMinor : Integer;
function rewRegExprVersionStr   : String;

implementation

function rewRegExpr(const aInp, aExpr : String) : Boolean;
var re : TRegExpr;
begin
  re := TRegExpr.Create(RegExprString(aExpr));
  re.ModifierR := true;
  try
    Result := re.Exec(RegExprString(aInp));
  finally
    re.Free;
  end;
end;

function rewReplace(const ARegExpr, AInputStr, AReplaceStr: String;
  AUseSubstitution: boolean): String;
begin
  Result := UTF8Encode(ReplaceRegExpr(RegExprString(ARegExpr),
                                      RegExprString(AInputStr),
                                      RegExprString(AReplaceStr),
                                      AUseSubstitution));
end;

function rewRegExprVersionMajor : Integer;
begin
  Result := TRegExpr.VersionMajor;
end;

function rewRegExprVersionMinor : Integer;
begin
  Result := TRegExpr.VersionMinor;
end;

function rewRegExprVersionStr : String;
begin
  Result := IntToStr(rewRegExprVersionMajor) + '.' +
            IntToStr(rewRegExprVersionMinor);
end;

{ TRegExprWrapper }

function TRegExprWrapper.GetMatch(index : integer): String;
begin
  Result := UTF8Encode(rexp.Match[index]);
end;

function TRegExprWrapper.GetMatchLen(index : integer): integer;
begin
  Result := rexp.MatchLen[index];
end;

function TRegExprWrapper.GetMatchPos(index : integer): integer;
begin
  Result := rexp.MatchPos[index];
end;

constructor TRegExprWrapper.Create(const Expr: String);
begin
  rexp := TRegExpr.Create(RegExprString(Expr));
end;

function TRegExprWrapper.Exec(const Expr: String): Boolean;
begin
  Result := rexp.Exec(RegExprString(Expr));
end;

function TRegExprWrapper.ExecNext: Boolean;
begin
  Result := rexp.ExecNext;
end;

function TRegExprWrapper.SubExprMatchCount: Integer;
begin
  Result := rexp.SubExprMatchCount;
end;

procedure TRegExprWrapper.SetModifierR;
begin
  rexp.ModifierR := true;
end;

procedure TRegExprWrapper.SetModifierM;
begin
  rexp.ModifierM := true;
end;

procedure TRegExprWrapper.SetModifierI;
begin
  rexp.ModifierI := true;
end;

procedure TRegExprWrapper.SetModifierG;
begin
  rexp.ModifierG := true;
end;

class function TRegExprWrapper.GetRusSmallLettersRange: String;
begin
  {$ifdef regexpr_nt}
  result := '[а-я]';
  {$else}
  result := '[а-пр-я]';
  {$endif}
end;

destructor TRegExprWrapper.Destroy;
begin
  rexp.Free;
  inherited Destroy;
end;

end.

