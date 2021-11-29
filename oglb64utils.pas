{ OGLB64Utils
   Base64 conversions
   Copyright (c) 2021 Ilya Medvedkov   }

unit OGLB64Utils;

{$mode objfpc}{$H+}

interface

function EncodeIntToB64(value : Cardinal; Digits : integer) : RawByteString;
function EncodeInt64ToB64(value : QWORD; Digits : integer) : String;
function DecodeB64ToByte(const C : AnsiChar) : Byte; inline;
function DecodeB64ToInt(const value : RawByteString) : Cardinal; overload;
function DecodeB64ToInt64(const value : RawByteString) : QWORD; overload;
function DecodeB64ToInt(const value : PChar; l : Integer) : Cardinal; overload;
function DecodeB64ToInt64(const value : PChar; l : Integer) : QWORD; overload;

implementation

{$J-}
const b64d62 = '.';
const b64d63 = '_';
const b64EncodeTable : Array [0..63] of AnsiChar =
                                       ('0','1','2','3','4','5','6','7','8','9',
                                        'a','b','c','d','e','f','g','h','i','j',
                                        'k','l','m','n','o','p','q','r','s','t',
                                        'u','v','w','x','y','z','A','B','C','D',
                                        'E','F','G','H','I','J','K','L','M','N',
                                        'O','P','Q','R','S','T','U','V','W','X',
                                        'Y','Z',b64d62, b64d63);

function EncodeIntToB64(value : Cardinal; Digits : integer) : RawByteString;
var i: integer;
begin
 If Digits=0 then
   Digits:=1;
 SetLength(result, digits);
 for i := 0 to digits - 1 do
  begin
   result[digits - i] := b64EncodeTable[value and 63];
   value := value shr 6;
  end ;
 while value <> 0 do begin
   result := b64EncodeTable[value and 63] + result;
   value := value shr 6;
 end;
end;

function EncodeInt64ToB64(value : QWORD; Digits : integer) : String;
var i: integer;
begin
 If Digits=0 then
   Digits:=1;
 SetLength(result, digits);
 for i := 0 to digits - 1 do
  begin
   result[digits - i] := b64EncodeTable[value and 63];
   value := value shr 6;
  end ;
 while value <> 0 do begin
   result := b64EncodeTable[value and 63] + result;
   value := value shr 6;
 end;
end;

function DecodeB64ToByte(const C : AnsiChar) : Byte; inline;
begin
  case C of
  '0'..'9' : Result := Ord(C) - Ord('0');
  'a'..'z' : Result := Ord(C) - Ord('a') + 10;
  'A'..'Z' : Result := Ord(C) - Ord('A') + 36;
  b64d62 : Result := 62;
  b64d63 : Result := 63;
  end;
end;

function DecodeB64ToInt(const value : RawByteString) : Cardinal;
begin
 Result := DecodeB64ToInt(@(value[1]), Length(value));
end;

function DecodeB64ToInt64(const value : RawByteString) : QWORD;
begin
  Result := DecodeB64ToInt64(@(value[1]), Length(value));
end;

function DecodeB64ToInt(const value : PChar; l : Integer) : Cardinal;
var i : integer;
begin
  Result := 0;
  for i := 0 to l-1 do
    Result := (Result shl 6) or DecodeB64ToByte(value[i]);
end;

function DecodeB64ToInt64(const value : PChar; l : Integer) : QWORD;
var i : integer;
begin
  Result := 0;
  for i := 0 to l-1 do
    Result := (Result shl 6) or DecodeB64ToByte(value[i]);
end;

end.

