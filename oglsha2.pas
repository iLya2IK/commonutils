unit OGLSHA2;

{$mode objfpc}{$H+}

interface

uses OGLGOSTCrypt;

type
  TSHA256Digest = array[0..31] of Byte;
  TSHA512Digest = array[0..63] of Byte;

  TSHA256Context = record
    State: array[0..7] of DWord;
    Buffer: array[0..63] of Byte;
    LenHi, LenLo : LongWord;
    Index : DWord;
  end;

  TSHA512Context = record
    State : array[0..7] of QWord;
    Buffer: array[0..127] of Byte;
    LenHi, LenLo : QWord;
    Index : DWord;
  end;

  TGOST94Context = record
    len : Int64;
    cipher_ctx : TGOST89Context;
    left : integer;
    H, S, remainder : Array [0..31] of byte;
  end;

{ core }
procedure SHA256Init(out ctx: TSHA256Context);
procedure SHA256Update(var ctx: TSHA256Context; const Buf; BufLen: PtrUInt);
procedure SHA256Final(var ctx: TSHA256Context; out Digest: TSHA256Digest);
procedure SHA512Init(out ctx: TSHA512Context);
procedure SHA512Update(var ctx: TSHA512Context; const Buf; BufLen: PtrUInt);
procedure SHA512Final(var ctx: TSHA512Context; out Digest: TSHA512Digest);
procedure GOST94Init(out ctx: TGOST94Context);
procedure GOST94Update(var ctx: TGOST94Context; const Buf; BufLen: PtrUInt);
procedure GOST94Final(var ctx: TGOST94Context; out Digest: TSHA256Digest);

{ auxiliary }
function SHA256String(const S: String): TSHA256Digest;
function SHA256Buffer(const Buf; BufLen: PtrUInt): TSHA256Digest;
function SHA512String(const S: String): TSHA512Digest;
function SHA512Buffer(const Buf; BufLen: PtrUInt): TSHA512Digest;
function GOST94String(const S: String): TSHA256Digest;
function GOST94Buffer(const Buf; BufLen: PtrUInt): TSHA256Digest;

{ helpers }
function SHA256Print(const Digest: TSHA256Digest): String;
function SHA256Match(const Digest1, Digest2: TSHA256Digest): Boolean;
function SHA512Print(const Digest: TSHA512Digest): String;
function SHA512Match(const Digest1, Digest2: TSHA512Digest): Boolean;

implementation
uses sysutils;

{$R-}{$Q-}

const SHA256_BUFFER_SIZE = 64;
      SHA256_HASH_SIZE   = 32;
      SHA512_BUFFER_SIZE = 128;
      SHA512_HASH_SIZE   = 64;

      HexTbl: array[0..15] of char='0123456789abcdef';     // lowercase

function SwapDWord(a: dword): dword; inline;
begin
  Result:= ((a and $FF) shl 24) or ((a and $FF00) shl 8) or ((a and $FF0000) shr 8) or ((a and $FF000000) shr 24);
end;

function SwapQWord(a: qword): qword; inline;
begin
  Result:= ((a and $FF) shl 56) or ((a and $FF00) shl 40) or ((a and $FF0000) shl 24) or ((a and $FF000000) shl 8) or
    ((a and $FF00000000) shr 8) or ((a and $FF0000000000) shr 24) or ((a and $FF000000000000) shr 40) or ((a and $FF00000000000000) shr 56);
end;

procedure SHA256Compress(var ctx: TSHA256Context);
var
  a, b, c, d, e, f, g, h, t1, t2: DWord;
  W: array [0..63] of DWord;
  i: longword;
begin
  ctx.Index:= 0;
  a:= ctx.State[0]; b:= ctx.State[1]; c:= ctx.State[2]; d:= ctx.State[3];
  e:= ctx.State[4]; f:= ctx.State[5]; g:= ctx.State[6]; h:= ctx.State[7];
  Move(ctx.Buffer, W, SHA256_BUFFER_SIZE);
  for i:= 0 to 15 do
    W[i]:= SwapDWord(W[i]);
  for i:= 16 to 63 do
    W[i]:= (((W[i-2] shr 17) or (W[i-2] shl 15)) xor ((W[i-2] shr 19) or (W[i-2] shl 13)) xor
      (W[i-2] shr 10)) + W[i-7] + (((W[i-15] shr 7) or (W[i-15] shl 25)) xor
      ((W[i-15] shr 18) or (W[i-15] shl 14)) xor (W[i-15] shr 3)) + W[i-16];

  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $428a2f98 + W[0]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $71374491 + W[1]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $b5c0fbcf + W[2]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $e9b5dba5 + W[3]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $3956c25b + W[4]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $59f111f1 + W[5]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $923f82a4 + W[6]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $ab1c5ed5 + W[7]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $d807aa98 + W[8]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $12835b01 + W[9]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $243185be + W[10]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $550c7dc3 + W[11]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $72be5d74 + W[12]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $80deb1fe + W[13]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $9bdc06a7 + W[14]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $c19bf174 + W[15]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $e49b69c1 + W[16]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $efbe4786 + W[17]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $0fc19dc6 + W[18]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $240ca1cc + W[19]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $2de92c6f + W[20]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $4a7484aa + W[21]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $5cb0a9dc + W[22]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $76f988da + W[23]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $983e5152 + W[24]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $a831c66d + W[25]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $b00327c8 + W[26]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $bf597fc7 + W[27]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $c6e00bf3 + W[28]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $d5a79147 + W[29]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $06ca6351 + W[30]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $14292967 + W[31]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $27b70a85 + W[32]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $2e1b2138 + W[33]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $4d2c6dfc + W[34]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $53380d13 + W[35]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $650a7354 + W[36]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $766a0abb + W[37]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $81c2c92e + W[38]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $92722c85 + W[39]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $a2bfe8a1 + W[40]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $a81a664b + W[41]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $c24b8b70 + W[42]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $c76c51a3 + W[43]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $d192e819 + W[44]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $d6990624 + W[45]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $f40e3585 + W[46]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $106aa070 + W[47]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $19a4c116 + W[48]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $1e376c08 + W[49]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $2748774c + W[50]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $34b0bcb5 + W[51]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $391c0cb3 + W[52]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $4ed8aa4a + W[53]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $5b9cca4f + W[54]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $682e6ff3 + W[55]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;
  t1:= h + (((e shr 6) or (e shl 26)) xor ((e shr 11) or (e shl 21)) xor ((e shr 25) or (e shl 7))) + ((e and f) xor (not e and g)) + $748f82ee + W[56]; t2:= (((a shr 2) or (a shl 30)) xor ((a shr 13) or (a shl 19)) xor ((a shr 22) xor (a shl 10))) + ((a and b) xor (a and c) xor (b and c)); h:= t1 + t2; d:= d + t1;
  t1:= g + (((d shr 6) or (d shl 26)) xor ((d shr 11) or (d shl 21)) xor ((d shr 25) or (d shl 7))) + ((d and e) xor (not d and f)) + $78a5636f + W[57]; t2:= (((h shr 2) or (h shl 30)) xor ((h shr 13) or (h shl 19)) xor ((h shr 22) xor (h shl 10))) + ((h and a) xor (h and b) xor (a and b)); g:= t1 + t2; c:= c + t1;
  t1:= f + (((c shr 6) or (c shl 26)) xor ((c shr 11) or (c shl 21)) xor ((c shr 25) or (c shl 7))) + ((c and d) xor (not c and e)) + $84c87814 + W[58]; t2:= (((g shr 2) or (g shl 30)) xor ((g shr 13) or (g shl 19)) xor ((g shr 22) xor (g shl 10))) + ((g and h) xor (g and a) xor (h and a)); f:= t1 + t2; b:= b + t1;
  t1:= e + (((b shr 6) or (b shl 26)) xor ((b shr 11) or (b shl 21)) xor ((b shr 25) or (b shl 7))) + ((b and c) xor (not b and d)) + $8cc70208 + W[59]; t2:= (((f shr 2) or (f shl 30)) xor ((f shr 13) or (f shl 19)) xor ((f shr 22) xor (f shl 10))) + ((f and g) xor (f and h) xor (g and h)); e:= t1 + t2; a:= a + t1;
  t1:= d + (((a shr 6) or (a shl 26)) xor ((a shr 11) or (a shl 21)) xor ((a shr 25) or (a shl 7))) + ((a and b) xor (not a and c)) + $90befffa + W[60]; t2:= (((e shr 2) or (e shl 30)) xor ((e shr 13) or (e shl 19)) xor ((e shr 22) xor (e shl 10))) + ((e and f) xor (e and g) xor (f and g)); d:= t1 + t2; h:= h + t1;
  t1:= c + (((h shr 6) or (h shl 26)) xor ((h shr 11) or (h shl 21)) xor ((h shr 25) or (h shl 7))) + ((h and a) xor (not h and b)) + $a4506ceb + W[61]; t2:= (((d shr 2) or (d shl 30)) xor ((d shr 13) or (d shl 19)) xor ((d shr 22) xor (d shl 10))) + ((d and e) xor (d and f) xor (e and f)); c:= t1 + t2; g:= g + t1;
  t1:= b + (((g shr 6) or (g shl 26)) xor ((g shr 11) or (g shl 21)) xor ((g shr 25) or (g shl 7))) + ((g and h) xor (not g and a)) + $bef9a3f7 + W[62]; t2:= (((c shr 2) or (c shl 30)) xor ((c shr 13) or (c shl 19)) xor ((c shr 22) xor (c shl 10))) + ((c and d) xor (c and e) xor (d and e)); b:= t1 + t2; f:= f + t1;
  t1:= a + (((f shr 6) or (f shl 26)) xor ((f shr 11) or (f shl 21)) xor ((f shr 25) or (f shl 7))) + ((f and g) xor (not f and h)) + $c67178f2 + W[63]; t2:= (((b shr 2) or (b shl 30)) xor ((b shr 13) or (b shl 19)) xor ((b shr 22) xor (b shl 10))) + ((b and c) xor (b and d) xor (c and d)); a:= t1 + t2; e:= e + t1;

  ctx.State[0]:= ctx.State[0] + a;
  ctx.State[1]:= ctx.State[1] + b;
  ctx.State[2]:= ctx.State[2] + c;
  ctx.State[3]:= ctx.State[3] + d;
  ctx.State[4]:= ctx.State[4] + e;
  ctx.State[5]:= ctx.State[5] + f;
  ctx.State[6]:= ctx.State[6] + g;
  ctx.State[7]:= ctx.State[7] + h;
  FillChar(ctx.Buffer,SHA256_BUFFER_SIZE,0);
end;

procedure SHA512Compress(var ctx: TSHA512Context);
var
  a, b, c, d, e, f, g, h, t1, t2: QWord;
  W: array [0..79] of QWord;
  i: longword;
begin
  ctx.Index:= 0;
  a:= ctx.state[0]; b:= ctx.state[1]; c:= ctx.state[2]; d:= ctx.state[3];
  e:= ctx.state[4]; f:= ctx.state[5]; g:= ctx.state[6]; h:= ctx.state[7];
  Move(ctx.buffer,W,SHA512_BUFFER_SIZE);
  for i:= 0 to 15 do
    W[i]:= SwapQWord(W[i]);
  for i:= 16 to 79 do
    W[i]:= (((W[i-2] shr 19) or (W[i-2] shl 45)) xor ((W[i-2] shr 61) or (W[i-2] shl 3)) xor
      (W[i-2] shr 6)) + W[i-7] + (((W[i-15] shr 1) or (W[i-15] shl 63)) xor ((W[i-15] shr 8) or
      (W[i-15] shl 56)) xor (W[i-15] shr 7)) + W[i-16];

  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $428a2f98d728ae22 + W[0]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $7137449123ef65cd + W[1]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $b5c0fbcfec4d3b2f + W[2]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $e9b5dba58189dbbc + W[3]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $3956c25bf348b538 + W[4]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $59f111f1b605d019 + W[5]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $923f82a4af194f9b + W[6]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $ab1c5ed5da6d8118 + W[7]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $d807aa98a3030242 + W[8]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $12835b0145706fbe + W[9]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $243185be4ee4b28c + W[10]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $550c7dc3d5ffb4e2 + W[11]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $72be5d74f27b896f + W[12]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $80deb1fe3b1696b1 + W[13]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $9bdc06a725c71235 + W[14]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $c19bf174cf692694 + W[15]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $e49b69c19ef14ad2 + W[16]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $efbe4786384f25e3 + W[17]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $0fc19dc68b8cd5b5 + W[18]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $240ca1cc77ac9c65 + W[19]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $2de92c6f592b0275 + W[20]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $4a7484aa6ea6e483 + W[21]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $5cb0a9dcbd41fbd4 + W[22]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $76f988da831153b5 + W[23]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $983e5152ee66dfab + W[24]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $a831c66d2db43210 + W[25]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $b00327c898fb213f + W[26]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $bf597fc7beef0ee4 + W[27]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $c6e00bf33da88fc2 + W[28]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $d5a79147930aa725 + W[29]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $06ca6351e003826f + W[30]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $142929670a0e6e70 + W[31]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $27b70a8546d22ffc + W[32]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $2e1b21385c26c926 + W[33]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $4d2c6dfc5ac42aed + W[34]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $53380d139d95b3df + W[35]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $650a73548baf63de + W[36]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $766a0abb3c77b2a8 + W[37]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $81c2c92e47edaee6 + W[38]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $92722c851482353b + W[39]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $a2bfe8a14cf10364 + W[40]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $a81a664bbc423001 + W[41]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $c24b8b70d0f89791 + W[42]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $c76c51a30654be30 + W[43]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $d192e819d6ef5218 + W[44]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $d69906245565a910 + W[45]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $f40e35855771202a + W[46]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $106aa07032bbd1b8 + W[47]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $19a4c116b8d2d0c8 + W[48]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $1e376c085141ab53 + W[49]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $2748774cdf8eeb99 + W[50]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $34b0bcb5e19b48a8 + W[51]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $391c0cb3c5c95a63 + W[52]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $4ed8aa4ae3418acb + W[53]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $5b9cca4f7763e373 + W[54]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $682e6ff3d6b2b8a3 + W[55]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $748f82ee5defb2fc + W[56]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $78a5636f43172f60 + W[57]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $84c87814a1f0ab72 + W[58]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $8cc702081a6439ec + W[59]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $90befffa23631e28 + W[60]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $a4506cebde82bde9 + W[61]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $bef9a3f7b2c67915 + W[62]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $c67178f2e372532b + W[63]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $ca273eceea26619c + W[64]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $d186b8c721c0c207 + W[65]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $eada7dd6cde0eb1e + W[66]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $f57d4f7fee6ed178 + W[67]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $06f067aa72176fba + W[68]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $0a637dc5a2c898a6 + W[69]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $113f9804bef90dae + W[70]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $1b710b35131c471b + W[71]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;
  t1:= h + (((e shr 14) or (e shl 50)) xor ((e shr 18) or (e shl 46)) xor ((e shr 41) or (e shl 23))) + ((e and f) xor (not e and g)) + $28db77f523047d84 + W[72]; t2:= (((a shr 28) or (a shl 36)) xor ((a shr 34) or (a shl 30)) xor ((a shr 39) or (a shl 25))) + ((a and b) xor (a and c) xor (b and c)); d:= d + t1; h:= t1 + t2;
  t1:= g + (((d shr 14) or (d shl 50)) xor ((d shr 18) or (d shl 46)) xor ((d shr 41) or (d shl 23))) + ((d and e) xor (not d and f)) + $32caab7b40c72493 + W[73]; t2:= (((h shr 28) or (h shl 36)) xor ((h shr 34) or (h shl 30)) xor ((h shr 39) or (h shl 25))) + ((h and a) xor (h and b) xor (a and b)); c:= c + t1; g:= t1 + t2;
  t1:= f + (((c shr 14) or (c shl 50)) xor ((c shr 18) or (c shl 46)) xor ((c shr 41) or (c shl 23))) + ((c and d) xor (not c and e)) + $3c9ebe0a15c9bebc + W[74]; t2:= (((g shr 28) or (g shl 36)) xor ((g shr 34) or (g shl 30)) xor ((g shr 39) or (g shl 25))) + ((g and h) xor (g and a) xor (h and a)); b:= b + t1; f:= t1 + t2;
  t1:= e + (((b shr 14) or (b shl 50)) xor ((b shr 18) or (b shl 46)) xor ((b shr 41) or (b shl 23))) + ((b and c) xor (not b and d)) + $431d67c49c100d4c + W[75]; t2:= (((f shr 28) or (f shl 36)) xor ((f shr 34) or (f shl 30)) xor ((f shr 39) or (f shl 25))) + ((f and g) xor (f and h) xor (g and h)); a:= a + t1; e:= t1 + t2;
  t1:= d + (((a shr 14) or (a shl 50)) xor ((a shr 18) or (a shl 46)) xor ((a shr 41) or (a shl 23))) + ((a and b) xor (not a and c)) + $4cc5d4becb3e42b6 + W[76]; t2:= (((e shr 28) or (e shl 36)) xor ((e shr 34) or (e shl 30)) xor ((e shr 39) or (e shl 25))) + ((e and f) xor (e and g) xor (f and g)); h:= h + t1; d:= t1 + t2;
  t1:= c + (((h shr 14) or (h shl 50)) xor ((h shr 18) or (h shl 46)) xor ((h shr 41) or (h shl 23))) + ((h and a) xor (not h and b)) + $597f299cfc657e2a + W[77]; t2:= (((d shr 28) or (d shl 36)) xor ((d shr 34) or (d shl 30)) xor ((d shr 39) or (d shl 25))) + ((d and e) xor (d and f) xor (e and f)); g:= g + t1; c:= t1 + t2;
  t1:= b + (((g shr 14) or (g shl 50)) xor ((g shr 18) or (g shl 46)) xor ((g shr 41) or (g shl 23))) + ((g and h) xor (not g and a)) + $5fcb6fab3ad6faec + W[78]; t2:= (((c shr 28) or (c shl 36)) xor ((c shr 34) or (c shl 30)) xor ((c shr 39) or (c shl 25))) + ((c and d) xor (c and e) xor (d and e)); f:= f + t1; b:= t1 + t2;
  t1:= a + (((f shr 14) or (f shl 50)) xor ((f shr 18) or (f shl 46)) xor ((f shr 41) or (f shl 23))) + ((f and g) xor (not f and h)) + $6c44198c4a475817 + W[79]; t2:= (((b shr 28) or (b shl 36)) xor ((b shr 34) or (b shl 30)) xor ((b shr 39) or (b shl 25))) + ((b and c) xor (b and d) xor (c and d)); e:= e + t1; a:= t1 + t2;

  ctx.state[0]:= ctx.state[0] + a;
  ctx.state[1]:= ctx.state[1] + b;
  ctx.state[2]:= ctx.state[2] + c;
  ctx.state[3]:= ctx.state[3] + d;
  ctx.state[4]:= ctx.state[4] + e;
  ctx.state[5]:= ctx.state[5] + f;
  ctx.state[6]:= ctx.state[6] + g;
  ctx.state[7]:= ctx.state[7] + h;
  FillChar(ctx.buffer,SHA512_BUFFER_SIZE,0);
end;


procedure SHA256Init(out ctx: TSHA256Context);
begin
  FillChar(ctx, sizeof(TSHA256Context), 0);
  ctx.State[0]:= $6a09e667;
  ctx.State[1]:= $bb67ae85;
  ctx.State[2]:= $3c6ef372;
  ctx.State[3]:= $a54ff53a;
  ctx.State[4]:= $510e527f;
  ctx.State[5]:= $9b05688c;
  ctx.State[6]:= $1f83d9ab;
  ctx.State[7]:= $5be0cd19;
end;

procedure SHA256Update(var ctx: TSHA256Context; const Buf; BufLen: PtrUInt);
var
  PBuf: ^byte;
begin
  Inc(ctx.LenHi,BufLen shr 29);
  Inc(ctx.LenLo,BufLen*8);
  if ctx.LenLo< (BufLen*8) then
    Inc(ctx.LenHi);

  PBuf:= @Buf;
  while BufLen > 0 do
  begin
    if (SHA256_BUFFER_SIZE-ctx.Index) <= DWord(BufLen) then
    begin
      Move(PBuf^,ctx.Buffer[ctx.Index],SHA256_BUFFER_SIZE-ctx.Index);
      Dec(BufLen,SHA256_BUFFER_SIZE-ctx.Index);
      Inc(PBuf,SHA256_BUFFER_SIZE-ctx.Index);
      SHA256Compress(ctx);
    end
    else
    begin
      Move(PBuf^,ctx.Buffer[ctx.Index],BufLen);
      Inc(ctx.Index, BufLen);
      BufLen:= 0;
    end;
  end;
end;

procedure SHA256Final(var ctx: TSHA256Context; out Digest: TSHA256Digest);
begin
  ctx.Buffer[ctx.Index]:= $80;
  if ctx.Index >= 56 then
    SHA256Compress(ctx);
  PDWord(@ctx.Buffer[56])^:= SwapDWord(ctx.LenHi);
  PDWord(@ctx.Buffer[60])^:= SwapDWord(ctx.LenLo);
  SHA256Compress(ctx);
  ctx.state[0]:= SwapDWord(ctx.state[0]);
  ctx.state[1]:= SwapDWord(ctx.state[1]);
  ctx.state[2]:= SwapDWord(ctx.state[2]);
  ctx.state[3]:= SwapDWord(ctx.state[3]);
  ctx.state[4]:= SwapDWord(ctx.state[4]);
  ctx.state[5]:= SwapDWord(ctx.state[5]);
  ctx.state[6]:= SwapDWord(ctx.state[6]);
  ctx.state[7]:= SwapDWord(ctx.state[7]);
  Move(ctx.state,Digest,SHA256_HASH_SIZE);
end;

function SHA256String(const S: String): TSHA256Digest;
var
  Context: TSHA256Context;
begin
  SHA256Init(Context);
  SHA256Update(Context, PChar(S)^, length(S));
  SHA256Final(Context, Result);
end;

function SHA256Buffer(const Buf; BufLen: PtrUInt): TSHA256Digest;
var
  Context: TSHA256Context;
begin
  SHA256Init(Context);
  SHA256Update(Context, buf, buflen);
  SHA256Final(Context, Result);
end;

function SHA256Print(const Digest: TSHA256Digest): String;
var
  I: Integer;
  P: PChar;
begin
  SetLength(Result, 64);
  P := Pointer(Result);
  for I := 0 to 31 do
  begin
    P[0] := HexTbl[(Digest[i] shr 4) and 15];
    P[1] := HexTbl[Digest[i] and 15];
    Inc(P,2);
  end;
end;

function SHA256Match(const Digest1, Digest2: TSHA256Digest): Boolean;
{$ifdef CPU64}
var
  A: array[0..3] of QWord absolute Digest1;
  B: array[0..3] of QWord absolute Digest2;
begin
{$push}
{$B+}
  Result := (A[0] = B[0]) and (A[1] = B[1]) and (A[2] = B[2]) and (A[3] = B[3]);
{$pop}
end;
{$else}
var
  A: array[0..7] of DWord absolute Digest1;
  B: array[0..7] of DWord absolute Digest2;
begin
{$push}
{$B+}
  Result := (A[0] = B[0]) and (A[1] = B[1]) and
            (A[2] = B[2]) and (A[3] = B[3]) and
            (A[4] = B[4]) and (A[5] = B[5]) and
            (A[6] = B[6]) and (A[7] = B[7]);
{$pop}
end;
{$endif}

{ SHA512 }

procedure SHA512Init(out ctx: TSHA512Context);
begin
  FillChar(ctx, sizeof(TSHA512Context), 0);
  ctx.State[0]:= QWord($6a09e667f3bcc908);
  ctx.State[1]:= QWord($bb67ae8584caa73b);
  ctx.State[2]:= QWord($3c6ef372fe94f82b);
  ctx.State[3]:= QWord($a54ff53a5f1d36f1);
  ctx.State[4]:= QWord($510e527fade682d1);
  ctx.State[5]:= QWord($9b05688c2b3e6c1f);
  ctx.State[6]:= QWord($1f83d9abfb41bd6b);
  ctx.State[7]:= QWord($5be0cd19137e2179);
end;

procedure SHA512Update(var ctx: TSHA512Context; const Buf; BufLen: PtrUInt);
var
  PBuf: ^byte;
begin
  Inc(ctx.LenLo,BufLen*8);
  if ctx.LenLo < (BufLen*8) then
    Inc(ctx.LenHi);

  PBuf:= @Buf;
  while BufLen > 0 do
  begin
    if (SHA512_BUFFER_SIZE-ctx.Index)<= DWord(BufLen) then
    begin
      Move(PBuf^,ctx.Buffer[ctx.Index],SHA512_BUFFER_SIZE-ctx.Index);
      Dec(BufLen,SHA512_BUFFER_SIZE-ctx.Index);
      Inc(PBuf,SHA512_BUFFER_SIZE-ctx.Index);
      SHA512Compress(ctx);
    end
    else
    begin
      Move(PBuf^,ctx.Buffer[ctx.Index],BufLen);
      Inc(ctx.Index,BufLen);
      BufLen:= 0;
    end;
  end;
end;

procedure SHA512Final(var ctx: TSHA512Context; out Digest: TSHA512Digest);
begin
  ctx.Buffer[ctx.Index]:= $80;
  if ctx.Index >= 112 then
    SHA512Compress(ctx);
  PQWord(@ctx.Buffer[112])^:= SwapQWord(ctx.LenHi);
  PQWord(@ctx.Buffer[120])^:= SwapQWord(ctx.LenLo);
  SHA512Compress(ctx);
  ctx.state[0]:= SwapQWord(ctx.state[0]);
  ctx.state[1]:= SwapQWord(ctx.state[1]);
  ctx.state[2]:= SwapQWord(ctx.state[2]);
  ctx.state[3]:= SwapQWord(ctx.state[3]);
  ctx.state[4]:= SwapQWord(ctx.state[4]);
  ctx.state[5]:= SwapQWord(ctx.state[5]);
  ctx.state[6]:= SwapQWord(ctx.state[6]);
  ctx.state[7]:= SwapQWord(ctx.state[7]);
  Move(ctx.state,Digest,SHA512_HASH_SIZE);
end;

function SHA512String(const S: String): TSHA512Digest;
var
  Context: TSHA512Context;
begin
  SHA512Init(Context);
  SHA512Update(Context, PChar(S)^, length(S));
  SHA512Final(Context, Result);
end;

function SHA512Buffer(const Buf; BufLen: PtrUInt): TSHA512Digest;
var
  Context: TSHA512Context;
begin
  SHA512Init(Context);
  SHA512Update(Context, buf, buflen);
  SHA512Final(Context, Result);
end;

function SHA512Print(const Digest: TSHA512Digest): String;
var
  I: Integer;
  P: PChar;
begin
  SetLength(Result, SHA512_BUFFER_SIZE);
  P := Pointer(Result);
  for I := 0 to 63 do
  begin
    P[0] := HexTbl[(Digest[i] shr 4) and 15];
    P[1] := HexTbl[Digest[i] and 15];
    Inc(P,2);
  end;
end;

function SHA512Match(const Digest1, Digest2: TSHA512Digest): Boolean;
{$ifdef CPU64}
var
  A: array[0..7] of QWord absolute Digest1;
  B: array[0..7] of QWord absolute Digest2;
begin
{$push}
{$B+}
  Result := (A[0] = B[0]) and (A[1] = B[1]) and
            (A[2] = B[2]) and (A[3] = B[3]) and
            (A[4] = B[4]) and (A[5] = B[5]) and
            (A[6] = B[6]) and (A[7] = B[7]);
{$pop}
end;
{$else}
var
  A: array[0..15] of DWord absolute Digest1;
  B: array[0..15] of DWord absolute Digest2;
begin
{$push}
{$B+}
  Result := (A[0] = B[0]) and (A[1] = B[1]) and
            (A[2] = B[2]) and (A[3] = B[3]) and
            (A[4] = B[4]) and (A[5] = B[5]) and
            (A[6] = B[6]) and (A[7] = B[7]) and
            (A[8] = B[8]) and (A[9] = B[9]) and
            (A[10] = B[10]) and (A[11] = B[11]) and
            (A[12] = B[12]) and (A[13] = B[13]) and
            (A[14] = B[14]) and (A[15] = B[15]);
{$pop}
end;
{$endif}

procedure GOST94Init(out ctx: TGOST94Context);
begin

end;

procedure GOST94Update(var ctx: TGOST94Context; const Buf; BufLen: PtrUInt);
begin

end;

procedure GOST94Final(var ctx: TGOST94Context; out Digest: TSHA256Digest);
begin

end;

function GOST94String(const S: String): TSHA256Digest;
begin

end;

function GOST94Buffer(const Buf; BufLen: PtrUInt): TSHA256Digest;
begin

end;


end.

