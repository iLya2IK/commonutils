{ OGLGOSTCrypt
   Implementation of GOST 28147-89 encryption algorithm

   file: gost89.c
    Copyright (c) 2005-2006 Cryptocom LTD

   Translation from C to FreePascal by
    2023 Ilya Medvedkov   }

unit OGLGOSTCrypt;

{$mode objfpc}{$H+}

interface

type
  TGOST89CipherContext = record
    master_key : array [0..7] of Cardinal;
    key        : array [0..7] of Cardinal;
    mask       : array [0..7] of Cardinal;
    k87, k65, k43, k21 : array [0..255] of Cardinal;
  end;

  PGOST89CipherContext = ^TGOST89CipherContext;

  gost_subst_block = record
    k8,
    k7,
    k6,
    k5,
    k4,
    k3,
    k2,
    k1 : Array [0..15] of Byte;
  end;

  pgost_subst_block = ^gost_subst_block;

  TGOSTSubtKind = (gostR3411_94, gostR3411_94_CryptoPro, gost28147,
                   gost28147_CryptoProA, gost28147_CryptoProB,
                   gost28147_CryptoProC, gost28147_CryptoProD,
                   gost28147_TC26Z);

procedure GOST_Init(ctx : PGOST89CipherContext; subst_kind : TGOSTSubtKind);
procedure GOST_boxinit(c : PGOST89CipherContext; b : pgost_subst_block);
function  GOST_get_subst(subst_kind : TGOSTSubtKind) : pgost_subst_block;

{ Encrypt several full blocks in ECB mode }
procedure gost_enc(c : PGOST89CipherContext;
                   clear : PByte; cipher : PByte; blocks : Integer);
{ Decrypt several full blocks in ECB mode }
procedure gost_dec(c : PGOST89CipherContext;
                   cipher : PByte; clear : PByte; blocks : integer);
{ Encrypts several full blocks in CFB mode using 8cr_byte IV }
procedure gost_enc_cfb(ctx : PGOST89CipherContext;
                  const iv : PByte; const clear : PByte;
                  cipher : PByte; blocks : integer);
{ Decrypts several full blocks in CFB mode using 8cr_byte IV }
procedure gost_dec_cfb(ctx : PGOST89CipherContext;
                  const iv : PByte; const cipher : PByte;
                  clear : PByte; blocks : integer);
{ Encrypt one  block }
procedure gostcrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
{ Decrypt one  block }
procedure gostdecrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
{ Encrypt one  block }
procedure magmacrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
{ Decrypt one  block }
procedure magmadecrypt(c : PGOST89CipherContext;const in_ : PByte; out_ : PByte);
{ Set key into context }
procedure gost_key(c : PGOST89CipherContext; const k : PByte);
{ Set key into context without key mask }
procedure gost_key_nomask(c : PGOST89CipherContext; const k : PByte);
{ Set key into context }
procedure magma_key(c : PGOST89CipherContext; const k : PByte);
{ Set master 256-bit key to be used in TLSTREE calculation into context }
procedure magma_master_key(c : PGOST89CipherContext; const k : PByte);
{ Get key from context }
procedure gost_get_key(c : PGOST89CipherContext; k : PByte);
{ Intermediate function used for calculate hash }
procedure gost_enc_with_key(c : PGOST89CipherContext;
                            key, inblock, outblock : pByte);
{ Compute MAC of given length in bits from data }
function gost_mac( ctx : PGOST89CipherContext; mac_len : integer;
                   const data : PByte;
                   data_len : Cardinal; mac : PByte ) : integer;
{ Compute MAC of given length in bits from data, using non-zero 8-cr_byte IV
  (non-standard, for use in CryptoPro key transport only }
function gost_mac_iv(ctx : PGOST89CipherContext; mac_len : integer;
                   const iv : PByte;
                   const data : PByte; data_len : Cardinal;
                   mac : PByte ) : integer;
{ Perform one step of MAC calculation like gostcrypt }
procedure mac_block( c : PGOST89CipherContext; buffer : PByte; const block : PByte);
{ Extracts MAC value from mac state buffer }
procedure get_mac( buffer : PByte; nbits : Integer; out_ : PByte );
{ Implements cryptopro key meshing algorithm. Expect IV to be 8-cr_byte size}
procedure cryptopro_key_meshing( ctx : PGOST89CipherContext; iv : PByte );

var
  vDefaultGOST89CipherContext : TGOST89CipherContext;

implementation

const
  GostR3411_94_TestParamSet : gost_subst_block = (
      k8: ($1, $F, $D, $0, $5, $7, $A, $4, $9, $2, $3, $E, $6, $B, $8, $C);
      k7: ($D, $B, $4, $1, $3, $F, $5, $9, $0, $A, $E, $7, $6, $8, $2, $C);
      k6: ($4, $B, $A, $0, $7, $2, $1, $D, $3, $6, $8, $5, $9, $C, $F, $E);
      k5: ($6, $C, $7, $1, $5, $F, $D, $8, $4, $A, $9, $E, $0, $3, $B, $2);
      k4: ($7, $D, $A, $1, $0, $8, $9, $F, $E, $4, $6, $C, $B, $2, $5, $3);
      k3: ($5, $8, $1, $D, $A, $3, $4, $2, $E, $F, $C, $7, $6, $0, $9, $B);
      k2: ($E, $B, $4, $C, $6, $D, $F, $A, $2, $3, $8, $1, $0, $7, $5, $9);
      k1: ($4, $A, $9, $2, $D, $8, $0, $E, $6, $B, $1, $C, $7, $F, $5, $3)
  );

  { Substitution blocks for hash function 1.2.643.2.9.1.6.1  }
  GostR3411_94_CryptoProParamSet : gost_subst_block = (
      k8: ($1, $3, $A, $9, $5, $B, $4, $F, $8, $6, $7, $E, $D, $0, $2, $C);
      k7: ($D, $E, $4, $1, $7, $0, $5, $A, $3, $C, $8, $F, $6, $2, $9, $B);
      k6: ($7, $6, $2, $4, $D, $9, $F, $0, $A, $1, $5, $B, $8, $E, $C, $3);
      k5: ($7, $6, $4, $B, $9, $C, $2, $A, $1, $8, $0, $E, $F, $D, $3, $5);
      k4: ($4, $A, $7, $C, $0, $F, $2, $8, $E, $1, $6, $5, $D, $B, $9, $3);
      k3: ($7, $F, $C, $E, $9, $4, $1, $0, $3, $B, $5, $2, $6, $A, $8, $D);
      k2: ($5, $F, $4, $0, $2, $D, $B, $9, $1, $7, $6, $3, $C, $E, $A, $8);
      k1: ($A, $4, $5, $6, $8, $1, $3, $7, $D, $C, $E, $0, $9, $2, $B, $F)
  );

  { Test paramset from GOST 28147 }
  Gost28147_TestParamSet : gost_subst_block = (
      k8: ($C, $6, $5, $2, $B, $0, $9, $D, $3, $E, $7, $A, $F, $4, $1, $8);
      k7: ($9, $B, $C, $0, $3, $6, $7, $5, $4, $8, $E, $F, $1, $A, $2, $D);
      k6: ($8, $F, $6, $B, $1, $9, $C, $5, $D, $3, $7, $A, $0, $E, $2, $4);
      k5: ($3, $E, $5, $9, $6, $8, $0, $D, $A, $B, $7, $C, $2, $1, $F, $4);
      k4: ($E, $9, $B, $2, $5, $F, $7, $1, $0, $D, $C, $6, $A, $4, $3, $8);
      k3: ($D, $8, $E, $C, $7, $3, $9, $A, $1, $5, $2, $4, $6, $F, $0, $B);
      k2: ($C, $9, $F, $E, $8, $1, $3, $A, $2, $7, $4, $D, $6, $0, $B, $5);
      k1: ($4, $2, $F, $5, $9, $1, $0, $8, $E, $3, $B, $C, $D, $7, $A, $6)
  );

  { 1.2.643.2.2.31.1 }
  Gost28147_CryptoProParamSetA : gost_subst_block = (
      k8: ($B, $A, $F, $5, $0, $C, $E, $8, $6, $2, $3, $9, $1, $7, $D, $4);
      k7: ($1, $D, $2, $9, $7, $A, $6, $0, $8, $C, $4, $5, $F, $3, $B, $E);
      k6: ($3, $A, $D, $C, $1, $2, $0, $B, $7, $5, $9, $4, $8, $F, $E, $6);
      k5: ($B, $5, $1, $9, $8, $D, $F, $0, $E, $4, $2, $3, $C, $7, $A, $6);
      k4: ($E, $7, $A, $C, $D, $1, $3, $9, $0, $2, $B, $4, $F, $8, $5, $6);
      k3: ($E, $4, $6, $2, $B, $3, $D, $8, $C, $F, $5, $A, $0, $7, $1, $9);
      k2: ($3, $7, $E, $9, $8, $A, $F, $0, $5, $2, $6, $C, $B, $4, $D, $1);
      k1: ($9, $6, $3, $2, $8, $B, $1, $7, $A, $4, $E, $F, $C, $0, $D, $5)
  );

  { 1.2.643.2.2.31.2 }
  Gost28147_CryptoProParamSetB : gost_subst_block = (
      k8: ($0, $4, $B, $E, $8, $3, $7, $1, $A, $2, $9, $6, $F, $D, $5, $C);
      k7: ($5, $2, $A, $B, $9, $1, $C, $3, $7, $4, $D, $0, $6, $F, $8, $E);
      k6: ($8, $3, $2, $6, $4, $D, $E, $B, $C, $1, $7, $F, $A, $0, $9, $5);
      k5: ($2, $7, $C, $F, $9, $5, $A, $B, $1, $4, $0, $D, $6, $8, $E, $3);
      k4: ($7, $5, $0, $D, $B, $6, $1, $2, $3, $A, $C, $F, $4, $E, $9, $8);
      k3: ($E, $C, $0, $A, $9, $2, $D, $B, $7, $5, $8, $F, $3, $6, $1, $4);
      k2: ($0, $1, $2, $A, $4, $D, $5, $C, $9, $7, $3, $F, $B, $8, $6, $E);
      k1: ($8, $4, $B, $1, $3, $5, $0, $9, $2, $E, $A, $C, $D, $6, $7, $F)
  );

  { 1.2.643.2.2.31.3 }
  Gost28147_CryptoProParamSetC : gost_subst_block = (
      k8: ($7, $4, $0, $5, $A, $2, $F, $E, $C, $6, $1, $B, $D, $9, $3, $8);
      k7: ($A, $9, $6, $8, $D, $E, $2, $0, $F, $3, $5, $B, $4, $1, $C, $7);
      k6: ($C, $9, $B, $1, $8, $E, $2, $4, $7, $3, $6, $5, $A, $0, $F, $D);
      k5: ($8, $D, $B, $0, $4, $5, $1, $2, $9, $3, $C, $E, $6, $F, $A, $7);
      k4: ($3, $6, $0, $1, $5, $D, $A, $8, $B, $2, $9, $7, $E, $F, $C, $4);
      k3: ($8, $2, $5, $0, $4, $9, $F, $A, $3, $7, $C, $D, $6, $E, $1, $B);
      k2: ($0, $1, $7, $D, $B, $4, $5, $2, $8, $E, $F, $C, $9, $A, $6, $3);
      k1: ($1, $B, $C, $2, $9, $D, $0, $F, $4, $5, $8, $E, $A, $7, $6, $3)
  );

  { 1.2.643.2.2.31.4 }
  Gost28147_CryptoProParamSetD : gost_subst_block = (
      k8: ($1, $A, $6, $8, $F, $B, $0, $4, $C, $3, $5, $9, $7, $D, $2, $E);
      k7: ($3, $0, $6, $F, $1, $E, $9, $2, $D, $8, $C, $4, $B, $A, $5, $7);
      k6: ($8, $0, $F, $3, $2, $5, $E, $B, $1, $A, $4, $7, $C, $9, $D, $6);
      k5: ($0, $C, $8, $9, $D, $2, $A, $B, $7, $3, $6, $5, $4, $E, $F, $1);
      k4: ($1, $5, $E, $C, $A, $7, $0, $D, $6, $2, $B, $4, $9, $3, $F, $8);
      k3: ($1, $C, $B, $0, $F, $E, $6, $5, $A, $D, $4, $8, $9, $3, $7, $2);
      k2: ($B, $6, $3, $4, $C, $F, $E, $2, $7, $D, $8, $0, $5, $A, $9, $1);
      k1: ($F, $C, $2, $A, $6, $4, $5, $0, $7, $9, $E, $D, $1, $B, $8, $3)
  );

  { 1.2.643.7.1.2.5.1.1 }
  Gost28147_TC26ParamSetZ : gost_subst_block = (
      k8: ($1, $7, $e, $d, $0, $5, $8, $3, $4, $f, $a, $6, $9, $c, $b, $2);
      k7: ($8, $e, $2, $5, $6, $9, $1, $c, $f, $4, $b, $0, $d, $a, $3, $7);
      k6: ($5, $d, $f, $6, $9, $2, $c, $a, $b, $7, $8, $1, $4, $3, $e, $0);
      k5: ($7, $f, $5, $a, $8, $1, $6, $d, $0, $9, $3, $e, $b, $4, $2, $c);
      k4: ($c, $8, $2, $1, $d, $4, $f, $6, $7, $0, $a, $5, $3, $e, $9, $b);
      k3: ($b, $3, $5, $8, $2, $f, $a, $d, $e, $1, $7, $4, $c, $9, $6, $0);
      k2: ($6, $8, $2, $3, $9, $a, $5, $c, $1, $e, $4, $7, $b, $d, $0, $f);
      k1: ($c, $4, $6, $2, $a, $5, $b, $9, $e, $8, $d, $7, $0, $3, $f, $1)
  );

  CryptoProKeyMeshingKey : Array [0..31] of byte = (
      $69, $00, $72, $22, $64, $C9, $04, $23,
      $8D, $3A, $DB, $96, $46, $E9, $2A, $C4,
      $18, $FE, $AC, $94, $00, $ED, $07, $12,
      $C0, $86, $DC, $C2, $EF, $4C, $A9, $2B
  );

  ACPKM_D_const : Array [0..31] of byte = (
      $80, $81, $82, $83, $84, $85, $86, $87,
      $88, $89, $8A, $8B, $8C, $8D, $8E, $8F,
      $90, $91, $92, $93, $94, $95, $96, $97,
      $98, $99, $9A, $9B, $9C, $9D, $9E, $9F
  );

function GOST_get_subst(subst_kind: TGOSTSubtKind): pgost_subst_block;
begin
  case subst_kind of
    gostR3411_94           : Result := @GostR3411_94_TestParamSet;
    gostR3411_94_CryptoPro : Result := @GostR3411_94_CryptoProParamSet;
    gost28147              : Result := @Gost28147_TestParamSet;
    gost28147_CryptoProA   : Result := @Gost28147_CryptoProParamSetA;
    gost28147_CryptoProB   : Result := @Gost28147_CryptoProParamSetB;
    gost28147_CryptoProC   : Result := @Gost28147_CryptoProParamSetC;
    gost28147_CryptoProD   : Result := @Gost28147_CryptoProParamSetD;
    gost28147_TC26Z        : Result := @Gost28147_TC26ParamSetZ;
  else
    Result := @GostR3411_94_TestParamSet;
  end;
end;

{ Initalize context. Provides default value for subst_block }
procedure GOST_Init(ctx : PGOST89CipherContext; subst_kind : TGOSTSubtKind);
begin
  GOST_boxinit(ctx, GOST_get_subst(subst_kind));
end;

procedure GOST_boxinit(c : PGOST89CipherContext; b : pgost_subst_block);
var
  i : integer;
begin
  for i := 0 to 255 do
  begin
    c^.k87[i] := Cardinal(b^.k8[i shr 4] shl 4 or b^.k7[i and 15]) shl 24;
    c^.k65[i] := (b^.k6[i shr 4] shl 4 or b^.k5[i and 15]) shl 16;
    c^.k43[i] := (b^.k4[i shr 4] shl 4 or b^.k3[i and 15]) shl 8;
    c^.k21[i] := b^.k2[i shr 4] shl 4 or b^.k1[i and 15];
  end;
end;

{ Part of GOST 28147 algorithm moved into separate function }
function f(c : PGOST89CipherContext; x : Cardinal) : Cardinal; inline;
begin
    x := c^.k87[(x shr 24) and 255] or c^.k65[(x shr 16) and 255] or
         c^.k43[(x shr 8) and 255] or c^.k21[x and 255];
    { Rotate left 11 bits }
    Result := (x shl 11) or (x shr (21));
end;

{ Low-level encryption routine - encrypts one 64 bit block }
procedure gostcrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
var
  n1, n2 : Cardinal;
begin
  n1 := in_[0] or (in_[1] shl 8) or (in_[2] shl 16) or (cardinal(in_[3]) shl 24);
  n2 := in_[4] or (in_[5] shl 8) or (in_[6] shl 16) or (cardinal(in_[7]) shl 24);
  { Instead of swapping halves, swap names each round }

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);
  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);
  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);
  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  out_[0] := byte((n2 and $ff));
  out_[1] := byte(((n2 shr 8) and $ff));
  out_[2] := byte(((n2 shr 16) and $ff));
  out_[3] := byte((n2 shr 24));
  out_[4] := byte((n1 and $ff));
  out_[5] := byte(((n1 shr 8) and $ff));
  out_[6] := byte(((n1 shr 16) and $ff));
  out_[7] := byte((n1 shr 24));
end;

{ Low-level encryption routine - encrypts one 64 bit block }
procedure magmacrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
var
  n1, n2 : Cardinal;
begin
  n1 := in_[7-0] or (in_[7-1] shl 8) or (in_[7-2] shl 16) or (cardinal(in_[7-3]) shl 24);
  n2 := in_[7-4] or (in_[7-5] shl 8) or (in_[7-6] shl 16) or (cardinal(in_[7-7]) shl 24);
  { Instead of swapping halves, swap names each round }

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  out_[7-0] := byte( (n2 and $ff));
  out_[7-1] := byte( ((n2 shr 8) and $ff));
  out_[7-2] := byte( ((n2 shr 16) and $ff));
  out_[7-3] := byte( (n2 shr 24));
  out_[7-4] := byte( (n1 and $ff));
  out_[7-5] := byte( ((n1 shr 8) and $ff));
  out_[7-6] := byte( ((n1 shr 16) and $ff));
  out_[7-7] := byte( (n1 shr 24));
end;

{ Low-level decryption routine. Decrypts one 64-bit block }
procedure gostdecrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
var
  n1, n2 : Cardinal;
begin
  n1 := in_[0] or (in_[1] shl 8) or (in_[2] shl 16) or (cardinal(in_[3]) shl 24);
  n2 := in_[4] or (in_[5] shl 8) or (in_[6] shl 16) or (cardinal(in_[7]) shl 24);

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  out_[0] := byte(n2 and $ff);
  out_[1] := byte((n2 shr 8) and $ff);
  out_[2] := byte((n2 shr 16) and $ff);
  out_[3] := byte(n2 shr 24);
  out_[4] := byte(n1 and $ff);
  out_[5] := byte((n1 shr 8) and $ff);
  out_[6] := byte((n1 shr 16) and $ff);
  out_[7] := byte(n1 shr 24);
end;

{ Low-level decryption routine. Decrypts one 64-bit block }
procedure magmadecrypt(c : PGOST89CipherContext; const in_ : PByte; out_ : PByte);
var
  n1, n2 : Cardinal;
begin
  n1 := in_[7-0] or (in_[7-1] shl 8) or (in_[7-2] shl 16) or (cardinal( in_[7-3]) shl 24);
  n2 := in_[7-4] or (in_[7-5] shl 8) or (in_[7-6] shl 16) or (cardinal( in_[7-7]) shl 24);

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  n2 := n2 xor f(c, n1 + c^.key[7] + c^.mask[7]);
  n1 := n1 xor f(c, n2 + c^.key[6] + c^.mask[6]);
  n2 := n2 xor f(c, n1 + c^.key[5] + c^.mask[5]);
  n1 := n1 xor f(c, n2 + c^.key[4] + c^.mask[4]);
  n2 := n2 xor f(c, n1 + c^.key[3] + c^.mask[3]);
  n1 := n1 xor f(c, n2 + c^.key[2] + c^.mask[2]);
  n2 := n2 xor f(c, n1 + c^.key[1] + c^.mask[1]);
  n1 := n1 xor f(c, n2 + c^.key[0] + c^.mask[0]);

  out_[7-0] := byte(n2 and $ff);
  out_[7-1] := byte((n2 shr 8) and $ff);
  out_[7-2] := byte((n2 shr 16) and $ff);
  out_[7-3] := byte(n2 shr 24);
  out_[7-4] := byte(n1 and $ff);
  out_[7-5] := byte((n1 shr 8) and $ff);
  out_[7-6] := byte((n1 shr 16) and $ff);
  out_[7-7] := byte(n1 shr 24);
end;


{ Encrypts several blocks in_ ECB mode }
procedure gost_enc(c : PGOST89CipherContext; clear : PByte; cipher : PByte; blocks : integer);
var
  i : integer;
begin
  for i := 0 to blocks-1 do
  begin
    gostcrypt(c, clear, cipher);
    Inc(clear, 8);
    Inc(cipher, 8);
  end;
end;

{ Decrypts several blocks in_ ECB mode }
procedure gost_dec(c : PGOST89CipherContext; cipher : PByte; clear : PByte; blocks : integer);
var
  i : integer;
begin
  for i := 0 to blocks-1 do
  begin
    gostdecrypt(c, cipher, clear);
    Inc(clear, 8);
    Inc(cipher, 8);
  end;
end;

{ Encrypts several full blocks in_ CFB mode using 8byte IV }
procedure gost_enc_cfb(ctx : PGOST89CipherContext;
                  const iv : pbyte;
                  const clear : pbyte;
                  cipher : pbyte; blocks : integer);
var
  cur_iv : array [0..7] of byte;
  gamma  : array [0..7] of byte;
  i, j : integer;
  in_, out_ : PByte;
begin
  Move(iv^, cur_iv, 8);
  in_ := clear;
  out_ := cipher;
  i := 0;

  while i < blocks do
  begin
    gostcrypt(ctx, cur_iv, gamma);
    for j := 0 to 7 do
    begin
      out_[j] := in_[j] xor gamma[j];
      cur_iv[j] := out_[j];
    end;
    Inc(i);
    Inc(in_, 8);
    Inc(out_, 8);
  end;
end;

{ Decrypts several full blocks in_ CFB mode using 8byte IV }
procedure gost_dec_cfb(ctx : PGOST89CipherContext;
                  const iv : pByte;
                  const cipher: pByte;
                  clear : Pbyte;
                  blocks : integer);
var
  cur_iv : array [0..7] of byte;
  gamma  : array [0..7] of byte;
  i, j : integer;
  in_, out_ : PByte;
begin
  Move(iv^, cur_iv, 8);
  in_ := clear;
  out_ := cipher;
  i := 0;
  while i < blocks do
  begin
    gostcrypt(ctx, cur_iv, gamma);
    for j := 0 to 7 do
    begin
        cur_iv[j] := in_[j];
        out_[j] := cur_iv[j] xor gamma[j];
    end;
    Inc(i);
    Inc(in_, 8);
    Inc(out_, 8);
  end;
end;

procedure gost_key_impl(c : PGOST89CipherContext; const k : Pbyte);
var
  i, j : integer;
begin
  i := 0;
  j := 0;

  while (i < 8) do
  begin
    c^.key[i] :=
        (k[j] or (k[j + 1] shl 8) or (k[j + 2] shl 16) or
                                  (cardinal(k[j + 3]) shl 24)) - c^.mask[i];
    Inc(i);
    j += 4;
  end;
end;

{ Set 256 bit gost89 key into context without key mask }
procedure gost_key_nomask(c : PGOST89CipherContext; const k : Pbyte);
begin
  FillByte(c^.mask, sizeof(c^.mask), 0);
  gost_key_impl(c, k);
end;

{ Encrypts one block using specified key }
procedure gost_enc_with_key(c : PGOST89CipherContext;
                            key, inblock, outblock : pByte);
begin
  gost_key_nomask(c, key);
  gostcrypt(c, inblock, outblock);
end;

procedure RAND_priv_bytes(dst : pbyte; cnt : Integer);
var
  i : integer;
begin
  for i := 0 to cnt-1 do
    dst[i] := Random(256);
end;

{ Set 256 bit gost89 key into context }
procedure gost_key(c : PGOST89CipherContext; const k : Pbyte);
begin
  RAND_priv_bytes(@(c^.mask), sizeof(c^.mask));
  gost_key_impl(c, k);
end;

{ Set 256 bit Magma key into context }
procedure magma_key(c : PGOST89CipherContext; const k : Pbyte);
var
  i, j : integer;
begin
  RAND_priv_bytes(@(c^.mask), sizeof(c^.mask));
  i := 0;
  j := 0;

  while (i < 8) do
  begin
      c^.key[i] :=
          (k[j + 3] or (k[j + 2] shl 8) or (k[j + 1] shl 16) or
                                       (cardinal(k[j]) shl 24))  - c^.mask[i];
      Inc(i);
      j += 4;
  end;
end;

procedure magma_master_key(c : PGOST89CipherContext; const k : Pbyte);
begin
    Move(k^, c^.master_key, sizeof(c^.master_key));
end;

{ Retrieve 256-bit gost89 key from context }
procedure gost_get_key(c : PGOST89CipherContext; k : Pbyte);
var
  i, j : integer;
begin
  i := 0;
  j := 0;

  while (i < 8) do
  begin
    k[j] := byte((c^.key[i] + c^.mask[i]) and $FF);
    k[j+1] := byte(((c^.key[i] + c^.mask[i]) shr 8 )and $FF);
    k[j+2] := byte(((c^.key[i] + c^.mask[i]) shr 16) and $FF);
    k[j+3] := byte(((c^.key[i] + c^.mask[i]) shr 24) and $FF);
    Inc(i);
    j += 4;
  end;
end;

{ Retrieve 256-bit magma key from context }
procedure magma_get_key(c : PGOST89CipherContext; k : Pbyte);
var
  i, j : integer;
begin
  i := 0;
  j := 0;

  while (i < 8) do
  begin
    k[j + 3] := byte((c^.key[i] + c^.mask[i]) and $FF);
    k[j + 2] := byte(((c^.key[i] + c^.mask[i]) shr 8) and $FF);
    k[j + 1] := byte(((c^.key[i] + c^.mask[i]) shr 16) and $FF);
    k[j + 0] := byte(((c^.key[i] + c^.mask[i]) shr 24) and $FF);
    Inc(i);
    j += 4;
  end;
end;

{ Cleans up key from context }
procedure GOST_done(c : PGOST89CipherContext);
begin
  FillByte(c^.master_key, sizeof(c^.master_key), 0);
  FillByte(c^.key,        sizeof(c^.key), 0);
  FillByte(c^.mask,       sizeof(c^.mask), 0);
end;

{
  Compute GOST 28147 mac block Parameters gost_ctx *c - context initalized
  with substitution blocks and key buffer - 8-byte mac state buffer block
  8-byte block to process.
 }
procedure mac_block(c : PGOST89CipherContext; buffer : Pbyte; const block : pbyte);
var
  n1, n2 : Cardinal;
  i : integer;
begin
  for i := 0 to 7 do
    buffer[i] := buffer[i] xor block[i];

  n1 := buffer[0] or (buffer[1] shl 8) or (buffer[2] shl 16) or (cardinal
                                                           (buffer[3]) shl 24);
  n2 := buffer[4] or (buffer[5] shl 8) or (buffer[6] shl 16) or (cardinal
                                                           (buffer[7]) shl 24);
  { Instead of swapping halves, swap names each round }

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  n2 := n2 xor f(c, n1 + c^.key[0] + c^.mask[0]);
  n1 := n1 xor f(c, n2 + c^.key[1] + c^.mask[1]);
  n2 := n2 xor f(c, n1 + c^.key[2] + c^.mask[2]);
  n1 := n1 xor f(c, n2 + c^.key[3] + c^.mask[3]);
  n2 := n2 xor f(c, n1 + c^.key[4] + c^.mask[4]);
  n1 := n1 xor f(c, n2 + c^.key[5] + c^.mask[5]);
  n2 := n2 xor f(c, n1 + c^.key[6] + c^.mask[6]);
  n1 := n1 xor f(c, n2 + c^.key[7] + c^.mask[7]);

  buffer[0] := byte(n1 and $ff);
  buffer[1] := byte((n1 shr 8) and $ff);
  buffer[2] := byte((n1 shr 16) and $ff);
  buffer[3] := byte(n1 shr 24);
  buffer[4] := byte(n2 and $ff);
  buffer[5] := byte((n2 shr 8) and $ff);
  buffer[6] := byte((n2 shr 16) and $ff);
  buffer[7] := byte(n2 shr 24);
end;

{ Get mac with specified number of bits from MAC state buffer }
procedure get_mac(buffer : pbyte; nbits : integer; out_ : pbyte);
var
  nbytes,rembits,mask,i : integer;
begin
  nbytes := nbits shr 3;
  rembits := nbits and 7;
  if (rembits > 0) then
    mask := (byte(1 < rembits) - 1)
  else
    mask := 0;
  for i := 0 to nbytes-1 do
    out_[i] := buffer[i];
  if (rembits > 0) then
    out_[i] := buffer[i] and mask;
end;

{
  Compute mac of specified length (in_ bits) from data. Context should be
  initialized with key and subst blocks
 }
function gost_mac(ctx : PGOST89CipherContext; mac_len : integer; const data : Pbyte;
                  data_len : Cardinal;
                  mac : pbyte) : integer;
const
  buffer : Array [0..7] of byte = ( 0, 0, 0, 0, 0, 0, 0, 0 );
var
  buf2 : Array [0..7] of byte;
  i : Cardinal;
begin
  i := 0;
  while ((i + 8) <= data_len) do
  begin
      mac_block(ctx, @buffer, @(data[i]));
      i += 8;
  end;
  if (i < data_len) then
  begin
      FillByte(buf2, 8, 0);
      Move(data[i], buf2, data_len - i);
      mac_block(ctx, @buffer, @buf2);
      i += 8;
  end;
  if (i = 8) then
  begin
      FillByte(buf2, 8, 0);
      mac_block(ctx, @buffer, @buf2);
  end;
  get_mac(@buffer, mac_len, mac);
  Exit(1);
end;

procedure OPENSSL_cleanse(c : PByte; sz : Cardinal);
begin
  FillByte(c^, sz, 0);
end;

{ Compute MAC with non-zero IV. Used in_ some RFC 4357 algorithms }
function gost_mac_iv(ctx : PGOST89CipherContext; mac_len : integer;
                  const iv : pByte;
                  const data : pbyte;
                  data_len : Cardinal;
                  mac : PByte) : Integer;
var
  buffer, buf2 : Array [0..7] of byte;
  i : Cardinal;
begin
  move(iv^, buffer, 8);
  i := 0;
  while ((i + 8) <= data_len) do
  begin
      mac_block(ctx, buffer, data + i);
      i += 8;
  end;
  if (i < data_len) then
  begin
      FillByte(buf2, 8, 0);
      Move(data[i], buf2, data_len - i);
      mac_block(ctx, @buffer, @buf2);
      i += 8;
  end;
  if (i = 8) then
  begin
      FillByte(buf2, 8, 0);
      mac_block(ctx, @buffer, @buf2);
  end;
  get_mac(@buffer, mac_len, mac);
  Exit(1);
end;

{ Implements key meshing algorithm by modifing ctx and IV in_ place }
procedure cryptopro_key_meshing(ctx : PGOST89CipherContext; iv : PByte);
var
  newkey : Array [0..31] of byte;
  newiv : Array [0..7] of byte;
begin
  { Set static keymeshing key }
  { "Decrypt" key with keymeshing key }
  gost_dec(ctx, @CryptoProKeyMeshingKey, @newkey, 4);
  { set new key }
  gost_key(ctx, newkey);
  OPENSSL_cleanse(newkey, sizeof(newkey));
  { Encrypt iv with new key }
  if assigned(iv) then
  begin
      gostcrypt(ctx, @iv, @newiv);
      Move(newiv, iv^, 8);
      OPENSSL_cleanse(newiv, sizeof(newiv));
  end;
end;

procedure acpkm_magma_key_meshing(ctx : PGOST89CipherContext);
var
  newkey : Array [0..31] of byte;
  i : integer;
begin
  for i := 0 to 3 do
      magmacrypt(ctx, @(ACPKM_D_const[8 * i]), @(newkey[8 * i]));
  { set new key }
  magma_key(ctx, @newkey);
  OPENSSL_cleanse(@newkey, sizeof(newkey));
end;

initialization
  GOST_Init(@vDefaultGOST89CipherContext, gostR3411_94);

finalization
  GOST_done(@vDefaultGOST89CipherContext);


end.

