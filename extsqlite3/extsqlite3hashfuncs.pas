{
 ExtSqlite3HashFuncs:
   Classes to realize additional sqlite functions.
   Additional functions to encode/decode strings/hashes vice versa.
   
   Part of ESolver project
   Copyright (c) 2023 by Ilya Medvedkov
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ExtSqlite3HashFuncs;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, ExtSqlite3DS, ctypes, OGLB64Utils;

type

  { TExprHashToB64EncodeFunction }

  TExprHashToB64EncodeFunction = class(TSqlite3Function)
  private
    function StrToHash(e : b64Encoder;
                         const S: String;
                         withPadding : Boolean) : String; virtual;
  public
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprHashToHexEncodeFunction }

  TExprHashToHexEncodeFunction = class(TSqlite3Function)
  private
    function StrToHash(const S: String) : String; virtual;
  public
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprSHA1ToB64EncodeFunction }

  TExprSHA1ToB64EncodeFunction = class(TExprHashToB64EncodeFunction)
  private
    function StrToHash(e : b64Encoder;
                         const S: String;
                         withPadding : Boolean) : String; override;
  public
    constructor Create;
  end;

  { TExprSHA256ToB64EncodeFunction }

  TExprSHA256ToB64EncodeFunction = class(TExprHashToB64EncodeFunction)
  private
    function StrToHash(e : b64Encoder;
                         const S: String;
                         withPadding : Boolean) : String; override;
  public
    constructor Create;
  end;

  { TExprSHA512ToB64EncodeFunction }

  TExprSHA512ToB64EncodeFunction = class(TExprHashToB64EncodeFunction)
  private
    function StrToHash(e : b64Encoder;
                         const S: String;
                         withPadding : Boolean) : String; override;
  public
    constructor Create;
  end;

  { TExprGOSTToB64EncodeFunction }

  TExprGOSTToB64EncodeFunction = class(TExprHashToB64EncodeFunction)
  private
    function StrToHash(e : b64Encoder;
                         const S: String;
                         withPadding : Boolean) : String; override;
  public
    constructor Create;
  end;

  { TExprSHA1EncodeFunction }

  TExprSHA1EncodeFunction = class(TExprHashToHexEncodeFunction)
  private
    function StrToHash(const S: String) : String; override;
  public
    constructor Create;
  end;

  { TExprSHA256EncodeFunction }

  TExprSHA256EncodeFunction = class(TExprHashToHexEncodeFunction)
  private
    function StrToHash(const S: String) : String; override;
  public
    constructor Create;
  end;

  { TExprSHA512EncodeFunction }

  TExprSHA512EncodeFunction = class(TExprHashToHexEncodeFunction)
  private
    function StrToHash(const S: String) : String; override;
  public
    constructor Create;
  end;

  { TExprGOSTEncodeFunction }

  TExprGOSTEncodeFunction = class(TExprHashToHexEncodeFunction)
  private
    function StrToHash(const S: String) : String; override;
  public
    constructor Create;
  end;

implementation

uses  SHA1, OGLSHA2, OGLGOSTHash;

{ TExprGOSTToB64EncodeFunction }

function TExprGOSTToB64EncodeFunction.StrToHash(e: b64Encoder; const S: String;
  withPadding: Boolean): String;
var
  b : TSHA256Digest;
begin
  b := SHA256String(S);
  Result := EncodeBufToB64Ext(e, b, SizeOf(b), withPadding);
end;

constructor TExprGOSTToB64EncodeFunction.Create;
begin
  inherited Create('gost_b64', -1, sqlteUtf8, sqlfScalar);
end;

{ TExprSHA512ToB64EncodeFunction }

function TExprSHA512ToB64EncodeFunction.StrToHash(e: b64Encoder;
  const S: String; withPadding: Boolean): String;
var
  b : TSHA512Digest;
begin
  b := SHA512String(S);
  Result := EncodeBufToB64Ext(e, b, SizeOf(b), withPadding);
end;

constructor TExprSHA512ToB64EncodeFunction.Create;
begin
  inherited Create('sha512_b64', -1, sqlteUtf8, sqlfScalar);
end;

{ TExprSHA256ToB64EncodeFunction }

function TExprSHA256ToB64EncodeFunction.StrToHash(e: b64Encoder;
  const S: String; withPadding: Boolean): String;
var
  b : TSHA256Digest;
begin
  b := SHA256String(S);
  Result := EncodeBufToB64Ext(e, b, SizeOf(b), withPadding);
end;

constructor TExprSHA256ToB64EncodeFunction.Create;
begin
  inherited Create('sha256_b64', -1, sqlteUtf8, sqlfScalar);
end;

{ TExprSHA1ToB64EncodeFunction }

function TExprSHA1ToB64EncodeFunction.StrToHash(e : b64Encoder;
                                                  const S: String;
                                                  withPadding : Boolean): String;
var
  b : TSHA1Digest;
begin
  b := SHA1String(S);
  Result := EncodeBufToB64Ext(e, b, SizeOf(b), withPadding);
end;

constructor TExprSHA1ToB64EncodeFunction.Create;
begin
  inherited Create('sha1_b64', -1, sqlteUtf8, sqlfScalar);
end;

{ TExprGOSTEncodeFunction }

function TExprGOSTEncodeFunction.StrToHash(const S: String): String;
begin
  Result := SHA256Print( GOST94String( AsString(0) ) );
end;

constructor TExprGOSTEncodeFunction.Create;
begin
  inherited Create('gost_enc', 1, sqlteUtf8, sqlfScalar);
end;

{ TExprSHA512EncodeFunction }

function TExprSHA512EncodeFunction.StrToHash(const S: String): String;
begin
  Result := SHA512Print( SHA512String( AsString(0) ) );
end;

constructor TExprSHA512EncodeFunction.Create;
begin
  inherited Create('sha512_enc', 1, sqlteUtf8, sqlfScalar);
end;

{ TExprSHA256EncodeFunction }

function TExprSHA256EncodeFunction.StrToHash(const S: String): String;
begin
  Result := SHA256Print( SHA256String( AsString(0) ) );
end;

constructor TExprSHA256EncodeFunction.Create;
begin
  inherited Create('sha256_enc', 1, sqlteUtf8, sqlfScalar);
end;

{ TExprSHA1EncodeFunction }

function TExprSHA1EncodeFunction.StrToHash(const S: String): String;
begin
  Result := SHA1Print( SHA1String( AsString(0) ) );
end;

constructor TExprSHA1EncodeFunction.Create;
begin
  inherited Create('sha1_enc', 1, sqlteUtf8, sqlfScalar);
end;

{ TExprHashToHexEncodeFunction }

function TExprHashToHexEncodeFunction.StrToHash(const S: String): String;
begin
  Result := '';
end;

procedure TExprHashToHexEncodeFunction.ScalarFunc(argc: integer);
begin
  if argc = 1 then
  begin
    SetResult( StrToHash( AsString(0) ) );
  end else
    SetResultNil;
end;

{ TExprHashToB64EncodeFunction }

function TExprHashToB64EncodeFunction.StrToHash(e: b64Encoder; const S: String;
  withPadding: Boolean): String;
begin
  Result := '';
end;

procedure TExprHashToB64EncodeFunction.ScalarFunc(argc: integer);
var
  S : String;
  e : b64Encoder;
  withPadding : Boolean;
begin
  if argc > 0 then
  begin
    if argc > 1 then
    begin
      S := AsString(1);
      if S = 'RFC3501' then
        e := b64RFC3501
      else
      if S = 'RFC4648' then
        e := b64RFC4648
      else
        e := b64RFC2045;

      if argc > 2 then
      begin
        withPadding := AsInt(2) > 0;
      end else
        withPadding := false;
    end else
      e := b64RFC2045;
    SetResult( StrToHash(e, AsString(0), withPadding) );
  end else
    SetResultNil;
end;


end.

