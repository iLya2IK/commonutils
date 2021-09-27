{
 ExprSqlite3Funcs:
   Set of functions for expression processing to use with 
   ExtSqlite3DS.TExtSqlite3Dataset

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}


unit ExprSqlite3Funcs;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, ExprComparator, ExtSqlite3DS;

type
  { TExprCompareFunction }

  TExprCompareFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprContainFunction }

  TExprContainFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprCrossHitRateFunction }

  TExprCrossHitRateFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprCrossHitContFunction }

  TExprCrossHitContFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;


implementation

{ TExprCrossHitContFunction }

constructor TExprCrossHitContFunction.Create;
begin
  inherited Create('dlcrscont', 3, sqlteUtf8, sqlfScalar);
end;

procedure TExprCrossHitContFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc in [2..3] then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      if argc = 2 then
        SetResult( E1.CrossHitContain(E2) ) else
        SetResult( E1.CrossHitContain(E2, AsInt(2)) )
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResult(null);
end;

{ TExprCrossHitRateFunction }

constructor TExprCrossHitRateFunction.Create;
begin
  inherited Create('dlcrs', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprCrossHitRateFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc = 2 then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      SetResult( E1.CrossHitRate(E2) );
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResult(null);
end;

{ TExprContainFunction }

constructor TExprContainFunction.Create;
begin
  inherited Create('dlcont', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprContainFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc = 2 then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      SetResult( E1.Contain(E2) );
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResult(null);
end;

{ TExprCompareFunction }

constructor TExprCompareFunction.Create;
begin
  inherited Create('dldist', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprCompareFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc = 2 then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      SetResult( E1.Compare(E2) );
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResult(null);
end;

end.

