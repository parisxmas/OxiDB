import S3Sidebar from '@/components/S3Sidebar'

export default function S3Layout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <div className="docs-shell">
      <S3Sidebar />
      <main className="docs-content">{children}</main>
    </div>
  )
}
