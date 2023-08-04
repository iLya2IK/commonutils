unit OGLGOSTCrypt;

{$mode objfpc}{$H+}

interface

type
  TGOST89Context = record
    master_key : array [0..7] of Cardinal;
    key        : array [0..7] of Cardinal;
    mask       : array [0..7] of Cardinal;
    k87, k65, k43, k21 : array [0..255] of Cardinal;
  end;

procedure GOST_Init(var ctx : TGOST89Context);

implementation

type
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

procedure kboxinit(var c : TGOST89Context; b : pgost_subst_block);
var
  i : integer;
begin
    for i := 0 to 255 do
    begin
        c.k87[i] := Cardinal(b^.k8[i shr 4] shl 4 or b^.k7[i and 15]) shl 24;
        c.k65[i] := (b^.k6[i shr 4] shl 4 or b^.k5[i and 15]) shl 16;
        c.k43[i] := (b^.k4[i shr 4] shl 4 or b^.k3[i and 15]) shl 8;
        c.k21[i] := b^.k2[i shr 4] shl 4 or b^.k1[i and 15];
    end;
end;

procedure GOST_Init(var ctx : TGOST89Context);
begin
  kboxinit(ctx, @GostR3411_94_TestParamSet);
end;

end.

