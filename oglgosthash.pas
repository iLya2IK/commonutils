{ OGLGOSTHash
   Implementation of GOST R 34.11-94 hash function

   file: gosthash.c
    Copyright (c) 2005-2006 Cryptocom LTD

   Translation from C to FreePascal by
    2023 Ilya Medvedkov   }

unit OGLGOSTHash;

{$mode objfpc}{$H+}

interface

uses
  OGLGOSTCrypt, OGLSHA2;

type
  TSHA256Digest = array[0..31] of Byte;

  TGOST94Context = record
    len  : Int64;
    cipher_ctx : PGOST89CipherContext;
    left : integer;
    H, S, remainder : Array [0..31] of byte;
  end;

  PGOST94Context = ^TGOST94Context;

{core}
procedure GOST94Init(out ctx: TGOST94Context);
procedure GOST94Update(var ctx: TGOST94Context; const Buf; BufLen: PtrUInt);
procedure GOST94Final(var ctx: TGOST94Context; out Digest: TSHA256Digest);

{ auxiliary }
function GOST94String(const S: String): TSHA256Digest;
function GOST94Buffer(const Buf; BufLen: PtrUInt): TSHA256Digest;

{ original gosthash.c routes}
function  init_gost_hash_ctx(ctx : PGOST94Context;
                       const subst_block : pgost_subst_block) : Integer;
function  start_gost_hash(ctx : PGOST94Context) : Integer;
function  gost_hash_block(ctx : PGOST94Context; block : PByte; length : PtrUInt) : integer;
function  finish_gost_hash(ctx : PGOST94Context; hashval : PByte) : Integer;
procedure done_gost_hash_ctx(ctx : PGOST94Context);

implementation

procedure swap_bytes(w, k : PByte);
var
  i, j : integer;
begin
  for i := 0 to 3 do
    for j := 0 to 7 do
      k[i + 4 * j] := w[8 * i + j];
end;

{ was A_A }
procedure circle_xor8(const w : pbyte; k : pbyte);
var
  i : integer;
  buf : Array [0..7] of byte;
begin
  move(w^, buf, 8);
  move(w[8], k^, 24);
  for i := 0 to 7 do
     k[i + 24] := buf[i] xor k[i];
end;

{ was R_R }
procedure transform_3(data : pbyte);
var
  acc : Word;
begin
  acc := (data[0] xor data[2] xor data[4] xor data[6] xor data[24] xor data[30]) or
        ((data[1] xor data[3] xor data[5] xor data[7] xor data[25] xor data[31]) shl 8);
  move(data[2], data^, 30);
  data[30] := acc and $ff;
  data[31] := acc shr 8;
end;

{ Adds blocks of N bytes modulo 2**(8*n). Returns carry }
function add_blocks(n : Integer; left : PByte; const right : PByte) : integer;
var
  i, carry, sum : integer;
begin
  carry := 0;
  for i := 0 to n-1 do
  begin
    sum := integer(left[i]) + integer(right[i]) + carry;
    left[i] := sum and $ff;
    carry := sum shr 8;
  end;
  Result := carry;
end;

{ Xor two sequences of bytes }
procedure xor_blocks(result : PByte; const a, b : PByte; len : Int64);
var
  i : Int32;
begin
  for i := 0 to len-1 do
    result[i] := a[i] xor b[i];
end;

{
  Calculate H(i+1) = Hash(Hi,Mi)
  Where H and M are 32 bytes long
 }
function hash_step(c : PGOST89CipherContext; H : PByte; const M : PByte) : Integer;
var
  i : integer;
  U, W, V, S, Key : Array [0..31] of Byte;
begin
  { Compute first key }
  xor_blocks(@W, H, M, 32);
  swap_bytes(@W, @Key);
  { Encrypt first 8 bytes of H with first key }
  gost_enc_with_key(c, @Key, H, @S);
  { Compute second key }
  circle_xor8(H, @U);
  circle_xor8(M, @V);
  circle_xor8(@V, @V);
  xor_blocks(@W, @U, @V, 32);
  swap_bytes(@W, @Key);
  { encrypt second 8 bytes of H with second key }
  gost_enc_with_key(c, @Key, @(H[8]), @(S[8]));
  { compute third key }
  circle_xor8(@U, @U);
  U[31] := not U[31];
  U[29] := not U[29];
  U[28] := not U[28];
  U[24] := not U[24];
  U[23] := not U[23];
  U[20] := not U[20];
  U[18] := not U[18];
  U[17] := not U[17];
  U[14] := not U[14];
  U[12] := not U[12];
  U[10] := not U[10];
  U[8]  := not U[8];
  U[7]  := not U[7];
  U[5]  := not U[5];
  U[3]  := not U[3];
  U[1]  := not U[1];
  circle_xor8(@V, @V);
  circle_xor8(@V, @V);
  xor_blocks(@W, @U, @V, 32);
  swap_bytes(@W, @Key);
  { encrypt third 8 bytes of H with third key }
  gost_enc_with_key(c, @Key, @(H[16]), @(S[16]));
  { Compute fourth key }
  circle_xor8(@U, @U);
  circle_xor8(@V, @V);
  circle_xor8(@V, @V);
  xor_blocks(@W, @U, @V, 32);
  swap_bytes(@W, @Key);
  { Encrypt last 8 bytes with fourth key }
  gost_enc_with_key(c, @Key, @(H[24]), @(S[24]));
  for i := 0 to 11 do
    transform_3(@S);
  xor_blocks(@S, @S, M, 32);
  transform_3(@S);
  xor_blocks(@S, @S, H, 32);
  for i := 0 to 60 do
    transform_3(@S);
  move(S, H^, 32);
  Result := 1;
end;

{
  Initialize gost_hash ctx - cleans up temporary structures and set up
  substitution blocks
 }
function init_gost_hash_ctx(ctx : PGOST94Context;
                       const subst_block : pgost_subst_block) : Integer;
begin
  FillByte(ctx^, sizeof(TGOST94Context), 0);
  ctx^.cipher_ctx := PGOST89CipherContext(GetMem(sizeof(TGOST89CipherContext)));
  if not Assigned(ctx^.cipher_ctx) then
      Exit(0);
  GOST_boxinit(ctx^.cipher_ctx, subst_block);
  Exit(1);
end;

{
  Free cipher CTX if it is dynamically allocated. Do not use
  if cipher ctx is statically allocated as in OpenSSL implementation of
  GOST hash algroritm
  }
procedure done_gost_hash_ctx(ctx : PGOST94Context);
begin
  Freemem(ctx^.cipher_ctx);
end;

{
  reset state of hash context to begin hashing new message
 }
function start_gost_hash(ctx : PGOST94Context) : Integer;
begin
  if not Assigned(ctx^.cipher_ctx) then
      Exit( 0 );
  FillByte(ctx^.H[0], 32, 0);
  FillByte(ctx^.S[0], 32, 0);
  ctx^.len := 0;
  ctx^.left := 0;
  Result := 1;
end;

{
  Hash block of arbitrary length
 }
function gost_hash_block(ctx : PGOST94Context; block : PByte; length : PtrUInt) : integer;
var
  add_bytes : Cardinal;
begin
  if (ctx^.left > 0) then
  begin
    {
     There are some bytes from previous step
     }
    add_bytes := 32 - ctx^.left;
    if (add_bytes > length) then
        add_bytes := length;
    move(block^, ctx^.remainder[ctx^.left], add_bytes);
    ctx^.left += add_bytes;
    if (ctx^.left < 32) then
       Exit(1);
    block += add_bytes;
    length -= add_bytes;
    hash_step(ctx^.cipher_ctx, @(ctx^.H), @(ctx^.remainder));
    add_blocks(32, @(ctx^.S), @(ctx^.remainder));
    ctx^.len += 32;
    ctx^.left := 0;
  end;

  while (length >= 32) do
  begin
    hash_step(ctx^.cipher_ctx, @(ctx^.H), block);

    add_blocks(32, @(ctx^.S), block);
    ctx^.len += 32;
    block += 32;
    length -= 32;
  end;
  if (length > 0) then
  begin
    ctx^.left := length;
    move(block^, ctx^.remainder, length);
  end;

  result := 1;
end;

{
  Compute hash value from current state of ctx
  state of hash ctx becomes invalid and cannot be used for further
  hashing.
 }
function finish_gost_hash(ctx : PGOST94Context; hashval : PByte) : Integer;
var
  buf, H, S : Array [0..31] of Byte;
  fin_len   : Int64;
  bptr : PByte;
begin
  fin_len := ctx^.len;
  move(ctx^.H, H, 32);
  move(ctx^.S, S, 32);
  if (ctx^.left > 0) then
  begin
    FillByte(buf, 32, 0);
    move(ctx^.remainder, buf, ctx^.left);
    hash_step(ctx^.cipher_ctx, @H, @buf);
    add_blocks(32, @S, @buf);
    fin_len += ctx^.left;
  end;

  FillByte(buf, 32, 0);
  if (fin_len = 0) then
    hash_step(ctx^.cipher_ctx, @H, @buf);
  bptr := @buf;
  fin_len := fin_len shl 3;   { Hash length }
  while (fin_len > 0) do
  begin
    bptr^ := byte(fin_len and $FF);
    Inc(bptr);
    fin_len := fin_len shr 8;
  end;
  hash_step(ctx^.cipher_ctx, @H, @buf);
  hash_step(ctx^.cipher_ctx, @H, @S);
  move(H, hashval^, 32);
  Result := 1;
end;

procedure GOST94Init(out ctx: TGOST94Context);
begin
  FillByte(ctx, sizeof(TGOST94Context), 0);
  ctx.cipher_ctx := @vDefaultGOST89CipherContext;
  start_gost_hash(@ctx);
end;

procedure GOST94Update(var ctx: TGOST94Context; const Buf; BufLen: PtrUInt);
begin
  gost_hash_block(@ctx, PByte(@buf), BufLen);
end;

procedure GOST94Final(var ctx: TGOST94Context; out Digest: TSHA256Digest);
begin
  finish_gost_hash(@ctx, @Digest);
end;

function GOST94String(const S: String): TSHA256Digest;
var
  Context: TGOST94Context;
begin
  GOST94Init(Context);
  GOST94Update(Context, PChar(S)^, length(S));
  GOST94Final(Context, Result);
end;

function GOST94Buffer(const Buf; BufLen: PtrUInt): TSHA256Digest;
var
  Context: TGOST94Context;
begin
  GOST94Init(Context);
  GOST94Update(Context, buf, buflen);
  GOST94Final(Context, Result);
end;

end.

