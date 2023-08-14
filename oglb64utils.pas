{ OGLB64Utils
   Base64 conversions
   Copyright (c) 2021-2023 Ilya Medvedkov

   Changes:
     08.2023 - RFC2045, RFC3501, RFC4648 base64 implementations added
     08.2023 - StrToB64/B64ToStr encoder/decoder added
   }

unit OGLB64Utils;

{$mode objfpc}{$H+}

interface

type
  b64Encoder = (b64OGL, b64RFC2045, b64RFC3501, b64RFC4648);

function EncodeBufToB64(const value; len : UIntPtr; withPadding : Boolean) : RawByteString;
function EncodeStrToB64(const value : RawByteString; withPadding : Boolean) : RawByteString;
function EncodeIntToB64(value : Cardinal; Digits : integer) : RawByteString;
function EncodeInt64ToB64(value : QWORD; Digits : integer) : String;
function DecodeB64ToStr(const value : RawByteString) : RawByteString;
function DecodeB64ToBuf(const value; Len : UIntPtr) : RawByteString;
function DecodeB64ToByte(const C : AnsiChar) : Byte; inline;
function DecodeB64ToInt(const value : RawByteString) : Cardinal; overload;
function DecodeB64ToInt64(const value : RawByteString) : QWORD; overload;
function DecodeB64ToInt(const value : PChar; l : Integer) : Cardinal; overload;
function DecodeB64ToInt64(const value : PChar; l : Integer) : QWORD; overload;

function EncodeBufToB64Ext(enc: b64Encoder; const value; len : UIntPtr; withPadding : Boolean) : RawByteString;
function EncodeStrToB64Ext(enc: b64Encoder; const value : RawByteString; withPadding : Boolean) : RawByteString;
function EncodeIntToB64Ext(enc: b64Encoder; value : Cardinal; Digits : integer) : RawByteString;
function EncodeInt64ToB64Ext(enc: b64Encoder; value : QWORD; Digits : integer) : String;
function DecodeB64ToStrExt(enc: b64Encoder; const value : RawByteString) : RawByteString;
function DecodeB64ToBufExt(enc: b64Encoder; const value; Len : UIntPtr) : RawByteString;
function DecodeB64ToByteExt(enc: b64Encoder; const C : AnsiChar) : Byte; inline;
function DecodeB64ToIntExt(enc: b64Encoder; const value : RawByteString) : Cardinal; overload;
function DecodeB64ToInt64Ext(enc: b64Encoder; const value : RawByteString) : QWORD; overload;
function DecodeB64ToIntExt(enc: b64Encoder; const value : PChar; l : Integer) : Cardinal; overload;
function DecodeB64ToInt64Ext(enc: b64Encoder; const value : PChar; l : Integer) : QWORD; overload;

var
  vB64DefaultEncoder : b64Encoder = b64OGL;

implementation

{$J-}
const b64d62 = '.';
const b64d63 = '_';
const b64EncodeTable : Array [b64Encoder, 0..63] of AnsiChar =
                                       (('0','1','2','3','4','5','6','7','8','9',
                                         'a','b','c','d','e','f','g','h','i','j',
                                         'k','l','m','n','o','p','q','r','s','t',
                                         'u','v','w','x','y','z','A','B','C','D',
                                         'E','F','G','H','I','J','K','L','M','N',
                                         'O','P','Q','R','S','T','U','V','W','X',
                                         'Y','Z',b64d62, b64d63),
                                        ('A','B','C','D','E','F','G','H','I','J',
                                         'K','L','M','N','O','P','Q','R','S','T',
                                         'U','V','W','X','Y','Z','a','b','c','d',
                                         'e','f','g','h','i','j','k','l','m','n',
                                         'o','p','q','r','s','t','u','v','w','x',
                                         'y','z','0','1','2','3','4','5','6','7',
                                         '8','9','+','/'),
                                        ('A','B','C','D','E','F','G','H','I','J',
                                         'K','L','M','N','O','P','Q','R','S','T',
                                         'U','V','W','X','Y','Z','a','b','c','d',
                                         'e','f','g','h','i','j','k','l','m','n',
                                         'o','p','q','r','s','t','u','v','w','x',
                                         'y','z','0','1','2','3','4','5','6','7',
                                         '8','9','+',','),
                                        ('A','B','C','D','E','F','G','H','I','J',
                                         'K','L','M','N','O','P','Q','R','S','T',
                                         'U','V','W','X','Y','Z','a','b','c','d',
                                         'e','f','g','h','i','j','k','l','m','n',
                                         'o','p','q','r','s','t','u','v','w','x',
                                         'y','z','0','1','2','3','4','5','6','7',
                                         '8','9','-','_'));

function EncodeBufToB64(const value; len: UIntPtr; withPadding: Boolean
  ): RawByteString;
begin

end;

function EncodeStrToB64(const value: RawByteString; withPadding: Boolean
  ): RawByteString;
begin
  Result := EncodeStrToB64Ext(vB64DefaultEncoder, value, withPadding);
end;

function EncodeIntToB64(value : Cardinal; Digits : integer) : RawByteString;
begin
  Result := EncodeIntToB64Ext(vB64DefaultEncoder, value, Digits);
end;

function EncodeInt64ToB64(value : QWORD; Digits : integer) : String;
begin
  Result := EncodeInt64ToB64Ext(vB64DefaultEncoder, value, Digits);
end;

function DecodeB64ToStr(const value: RawByteString): RawByteString;
begin
  Result := DecodeB64ToStrExt(vB64DefaultEncoder, value);
end;

function DecodeB64ToBuf(const value; Len: UIntPtr): RawByteString;
begin
  Result := DecodeB64ToBufExt(vB64DefaultEncoder, value, Len);
end;

function DecodeB64ToByte(const C : AnsiChar) : Byte; inline;
begin
  Result := DecodeB64ToByteExt(vB64DefaultEncoder, C);
end;

function DecodeB64ToInt(const value : RawByteString) : Cardinal;
begin
  Result := DecodeB64ToIntExt(vB64DefaultEncoder, @(value[1]), Length(value));
end;

function DecodeB64ToInt64(const value : RawByteString) : QWORD;
begin
  Result := DecodeB64ToInt64Ext(vB64DefaultEncoder, @(value[1]), Length(value));
end;

function DecodeB64ToInt(const value : PChar; l : Integer) : Cardinal;
begin
  Result := DecodeB64ToIntExt(vB64DefaultEncoder, @(value[1]), Length(value));
end;

function DecodeB64ToInt64(const value : PChar; l : Integer) : QWORD;
begin
  Result := DecodeB64ToIntExt(vB64DefaultEncoder, value, l);
end;

function SwapDWord(a: dword): dword; inline;
begin
  Result:= ((a and $FF) shl 24) or ((a and $FF00) shl 8) or ((a and $FF0000) shr 8) or ((a and $FF000000) shr 24);
end;

function EncodeBufToB64Ext(enc: b64Encoder; const value; len: UIntPtr;
  withPadding: Boolean): RawByteString;
var
  OutLen, I, P, offset : Integer;
  Buffer : Word;
  Buf : PByte;
begin
  if Len = 0 then Exit('');
  Buf := @value;
  OutLen := Len * 4 div 3;
  if (Len * 4 mod 3 > 0) then Inc(OutLen);
  SetLength(Result, OutLen);

  I := 0;
  P := 1;
  offset := 0;
  While I < Len do
  begin
    case offset of
    0 : begin
        buffer := Word(Buf[I]);
        Result[P] := b64EncodeTable[enc, (buffer shr 2) and 63];
        buffer := buffer shl 14;
        Inc(offset);
        Inc(I);
        Inc(P);
      end;
    1 : begin
        buffer := (Word(Buf[I]) shl 6) or buffer;
        Result[P] := b64EncodeTable[enc, (buffer shr 10) and 63];
        buffer := buffer shl 6;
        Inc(offset);
        Inc(I);
        Inc(P);
      end;
    2 : begin
        buffer := (Word(Buf[I]) shl 4) or buffer;
        Result[P] := b64EncodeTable[enc, (buffer shr 10) and 63];
        buffer := buffer shl 6;
        Inc(offset);
        Inc(I);
        Inc(P);
      end;
    3 : begin
        Result[P] := b64EncodeTable[enc, (buffer shr 10) and 63];
        Inc(P);
        buffer := 0;
        offset := 0;
      end;
    end;
  end;
  if offset > 0 then
    Result[P] := b64EncodeTable[enc, (buffer shr 10) and 63];
  if withPadding then
  begin
    offset := OutLen * 3 - Len * 4;
    while offset > 0 do
    begin
      Result := Result + '=';
      Dec(offset);
    end;
  end;
end;

function EncodeStrToB64Ext(enc: b64Encoder; const value: RawByteString;
  withPadding : Boolean): RawByteString;
begin
  if Length(value) = 0 then Exit('');
  Result := EncodeBufToB64Ext(enc, value[1], Length(value), withPadding);
end;

function EncodeIntToB64Ext(enc: b64Encoder; value: Cardinal; Digits: integer
  ): RawByteString;
var i: integer;
begin
 If Digits=0 then
   Digits:=1;
 SetLength(result, digits);
 for i := 0 to digits - 1 do
  begin
   result[digits - i] := b64EncodeTable[enc, value and 63];
   value := value shr 6;
  end ;
 while value <> 0 do begin
   result := b64EncodeTable[enc, value and 63] + result;
   value := value shr 6;
 end;
end;

function EncodeInt64ToB64Ext(enc: b64Encoder; value: QWORD; Digits: integer
  ): String;
var i: integer;
begin
 If Digits=0 then
   Digits:=1;
 SetLength(result, digits);
 for i := 0 to digits - 1 do
  begin
   result[digits - i] := b64EncodeTable[enc, value and 63];
   value := value shr 6;
  end ;
 while value <> 0 do begin
   result := b64EncodeTable[enc, value and 63] + result;
   value := value shr 6;
 end;
 for i := 0 to digits - 1 do
  begin
   result[digits - i] := b64EncodeTable[enc, value and 63];
   value := value shr 6;
  end ;
 while value <> 0 do begin
   result := b64EncodeTable[enc, value and 63] + result;
   value := value shr 6;
 end;
end;

function DecodeB64ToStrExt(enc: b64Encoder; const value: RawByteString
  ): RawByteString;
begin
  if Length(value) = 0 then Exit('');
  Result := DecodeB64ToBufExt(enc, value[1], Length(value))
end;

function DecodeB64ToBufExt(enc: b64Encoder; const value; Len: UIntPtr
  ): RawByteString;
var
  OutLen : Integer;
  i, p, offset : integer;
  Buffer : Cardinal;
  B : Byte;
  Buf : PAnsiChar;
begin
  Buf := @value;
  while ((Len > 0) and (Buf[Len-1] = '=')) do Dec(Len);
  if Len = 0 then Exit('');

  OutLen := Len * 3 div 4;
  if (Len * 3 mod 4) > 0 then Inc(OutLen);
  SetLength(Result, OutLen);

  I := 0;
  p := 1;
  offset := 0;
  while I < Len do
  begin
    case offset of
    0: begin
      B := DecodeB64ToByteExt(enc, Buf[i]);
      Inc(i);
      Buffer := Cardinal(B) shl 26;
      Inc(offset);
    end;
    1: begin
      B := DecodeB64ToByteExt(enc, Buf[i]);
      Inc(i);
      Buffer := (Cardinal(B) shl 20) or Buffer;
      Result[P] := Ansichar((Buffer shr 24) and $ff);
      inc(p);
      Inc(offset);
    end;
    2: begin
      B := DecodeB64ToByteExt(enc, Buf[i]);
      Inc(i);
      Buffer := (Cardinal(B) shl 14) or Buffer;
      Result[P] := Ansichar((Buffer shr 16) and $ff);
      inc(p);
      Inc(offset);
    end;
    3:  begin
      B := DecodeB64ToByteExt(enc, Buf[i]);
      Inc(i);
      Buffer := (Cardinal(B) shl 8) or Buffer;
      Result[P] := Ansichar((Buffer shr 8) and $ff);
      inc(p);
      offset := 0;
    end;
    end;
  end;
  case offset of
  1:
    Result[P] := Ansichar((Buffer shr 24) and $ff);
  2:
    Result[P] := Ansichar((Buffer shr 16) and $ff);
  3:
    Result[P] := Ansichar((Buffer shr 8)  and $ff);
  end;
end;

function DecodeB64ToByteExt(enc: b64Encoder; const C: AnsiChar): Byte; inline;
begin
  case enc of
  b64OGL: begin
    case C of
    '0'..'9' : Result := Ord(C) - Ord('0');
    'a'..'z' : Result := Ord(C) - Ord('a') + 10;
    'A'..'Z' : Result := Ord(C) - Ord('A') + 36;
    b64d62 : Result := 62;
    b64d63 : Result := 63;
    end;
  end;
  else
    case C of
    'A'..'Z' : Result := Ord(C) - Ord('A');
    'a'..'z' : Result := Ord(C) - Ord('a') + 26;
    '0'..'9' : Result := Ord(C) - Ord('0') + 52;
    else
      if C = b64EncodeTable[enc, 62] then Result := 62 else
                                          Result := 63;
    end;
  end;
end;

function DecodeB64ToIntExt(enc: b64Encoder; const value: RawByteString
  ): Cardinal;
begin
  Result := DecodeB64ToIntExt(enc, @(value[1]), Length(value));
end;

function DecodeB64ToInt64Ext(enc: b64Encoder; const value: RawByteString
  ): QWORD;
begin
  Result := DecodeB64ToInt64Ext(enc, @(value[1]), Length(value));
end;

function DecodeB64ToIntExt(enc: b64Encoder; const value: PChar; l: Integer
  ): Cardinal;
var i : integer;
begin
  Result := 0;
  for i := 0 to l-1 do
    Result := (Result shl 6) or DecodeB64ToByteExt(enc, value[i]);
end;

function DecodeB64ToInt64Ext(enc: b64Encoder; const value: PChar; l: Integer
  ): QWORD;
var i : integer;
begin
  Result := 0;
  for i := 0 to l-1 do
    Result := (Result shl 6) or DecodeB64ToByteExt(enc, value[i]);
end;

end.

